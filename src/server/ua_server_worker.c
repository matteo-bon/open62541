/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 *
 *    Copyright 2014-2017 (c) Fraunhofer IOSB (Author: Julius Pfrommer)
 *    Copyright 2014-2016 (c) Sten Grüner
 *    Copyright 2015 (c) Chris Iatrou
 *    Copyright 2015 (c) Nick Goossens
 *    Copyright 2015 (c) Jörg Schüler-Maroldt
 *    Copyright 2015-2016 (c) Oleksiy Vasylyev
 *    Copyright 2016-2017 (c) Florian Palm
 *    Copyright 2017 (c) Stefan Profanter, fortiss GmbH
 *    Copyright 2016 (c) Lorenz Haas
 *    Copyright 2017 (c) Jonas Green
 */

#include "ua_util.h"
#include "ua_server_internal.h"

#ifdef UA_ENABLE_VALGRIND_INTERACTIVE
#include <valgrind/memcheck.h>
#endif

#define UA_MAXTIMEOUT 50 /* Max timeout in ms between main-loop iterations */

/**
 * Worker Threads and Dispatch Queue
 * ---------------------------------
 * The worker threads dequeue callbacks from a central Multi-Producer
 * Multi-Consumer Queue (MPMC). When there are no callbacks, workers go idle.
 * The condition to wake them up is triggered whenever a callback is
 * dispatched.
 *
 * Future Plans: Use work-stealing to load-balance between cores.
 * Le, Nhat Minh, et al. "Correct and efficient work-stealing for weak memory
 * models." ACM SIGPLAN Notices. Vol. 48. No. 8. ACM, 2013. */

#ifdef UA_ENABLE_MULTITHREADING

static void *
workerLoop(UA_Worker *worker) {
    UA_Server *server = worker->server;
    UA_UInt32 *counter = &worker->counter;
    volatile UA_Boolean *running = &worker->running;

    /* Initialize the (thread local) random seed with the ram address
     * of the worker. Not for security-critical entropy! */
    UA_random_seed((uintptr_t)worker);

    while(*running) {
        UA_atomic_addUInt32(counter, 1);

        /* Remove a callback from the queue */
        pthread_mutex_lock(&server->dispatchQueue_accessMutex);
        UA_DelayedCallback *dc = SIMPLEQ_FIRST(&server->dispatchQueue);
        if(dc)
            SIMPLEQ_REMOVE_HEAD(&server->dispatchQueue, next);
        pthread_mutex_unlock(&server->dispatchQueue_accessMutex);

        /* Nothing to do. Sleep until a callback is dispatched */
        if(!dc) {
            pthread_mutex_lock(&server->dispatchQueue_conditionMutex);
            pthread_cond_wait(&server->dispatchQueue_condition,
                              &server->dispatchQueue_conditionMutex);
            pthread_mutex_unlock(&server->dispatchQueue_conditionMutex);
            continue;
        }

        /* Execute */
        if(dc->callback)
            dc->callback(server, dc->data);
        UA_free(dc);
    }

    UA_LOG_DEBUG(server->config.logger, UA_LOGCATEGORY_SERVER,
                 "Worker shut down");
    return NULL;
}

void UA_Server_cleanupDispatchQueue(UA_Server *server) {
    while(true) {
        pthread_mutex_lock(&server->dispatchQueue_accessMutex);
        UA_DelayedCallback *dc = SIMPLEQ_FIRST(&server->dispatchQueue);
        if(!dc) {
            pthread_mutex_unlock(&server->dispatchQueue_accessMutex);
            break;
        }
        SIMPLEQ_REMOVE_HEAD(&server->dispatchQueue, next);
        pthread_mutex_unlock(&server->dispatchQueue_accessMutex);
        dc->callback(server, dc->data);
        UA_free(dc);
    }
}

#endif

/* For multithreading, callbacks are not directly executed but enqueued for the
 * worker threads */

static void
executeCallback(UA_Server *server, UA_ServerCallback callback, void *data) {
#ifndef UA_ENABLE_MULTITHREADING
    /* Execute immediately */
    callback(server, data);
#else
    /* Execute immediately if memory could not be allocated */
    UA_DelayedCallback *dc = (UA_DelayedCallback*)UA_malloc(sizeof(UA_DelayedCallback));
    if(!dc) {
        callback(server, data);
        return;
    }

    dc->callback = callback;
    dc->data = data;

    /* Enqueue for the worker threads */
    pthread_mutex_lock(&server->dispatchQueue_accessMutex);
    SIMPLEQ_INSERT_TAIL(&server->dispatchQueue, dc, next);
    pthread_mutex_unlock(&server->dispatchQueue_accessMutex);

    /* Wake up sleeping workers */
    pthread_cond_broadcast(&server->dispatchQueue_condition);
#endif
}

/*********************/
/* Delayed Callbacks */
/*********************/

#ifdef UA_ENABLE_MULTITHREADING

/* Delayed Callbacks are called only when all callbacks that were dispatched
 * prior are finished. After every UA_MAX_DELAYED_SAMPLE delayed Callbacks that
 * were added to the queue, we sample the counters from the workers. The
 * counters are compared to the last counters that were sampled. If every worker
 * has proceeded the counter, then we know that all delayed callbacks prior to
 * the last sample-point are safe to execute. */

/* Sample the worker counter for every nth delayed callback. This is used to
 * test that all workers have **finished** their current job before the delayed
 * callback is processed. */
#define UA_MAX_DELAYED_SAMPLE 100

/* Call only with a held mutex for the delayed callbacks */
static void
dispatchDelayedCallbacks(UA_Server *server, UA_DelayedCallback *cb) {
    /* Are callbacks before the last checkpoint ready? */
    for(size_t i = 0; i < server->config.nThreads; ++i) {
        if(server->workers[i].counter == server->workers[i].checkpointCounter)
            return;
    }

    /* Dispatch all delayed callbacks up to the checkpoint.
     * TODO: Move over the entire queue up to the checkpoint in one step. */
    if(server->delayedCallbacks_checkpoint != NULL) {
        UA_DelayedCallback *iter, *tmp_iter;
        SIMPLEQ_FOREACH_SAFE(iter, &server->delayedCallbacks, next, tmp_iter) {
            pthread_mutex_lock(&server->dispatchQueue_accessMutex);
            SIMPLEQ_INSERT_TAIL(&server->dispatchQueue, iter, next);
            pthread_mutex_unlock(&server->dispatchQueue_accessMutex);
            if(iter == server->delayedCallbacks_checkpoint)
                break;
        }
    }

    /* Create the new sample point */
    for(size_t i = 0; i < server->config.nThreads; ++i)
        server->workers[i].checkpointCounter = server->workers[i].counter;
    server->delayedCallbacks_checkpoint = cb;
}

#endif

void
UA_Server_delayedCallback(UA_Server *server, UA_DelayedCallback *cb) {
#ifdef UA_ENABLE_MULTITHREADING
    pthread_mutex_lock(&server->dispatchQueue_accessMutex);
#endif
    SIMPLEQ_INSERT_HEAD(&server->delayedCallbacks, cb, next);
#ifdef UA_ENABLE_MULTITHREADING
    server->delayedCallbacks_sinceDispatch++;
    if(server->delayedCallbacks_sinceDispatch > UA_MAX_DELAYED_SAMPLE) {
        dispatchDelayedCallbacks(server, cb);
        server->delayedCallbacks_sinceDispatch = 0;
    }
    pthread_mutex_unlock(&server->dispatchQueue_accessMutex);
#endif
}

/* All workers are shut down at this point */
void UA_Server_cleanupDelayedCallbacks(UA_Server *server) {
    UA_DelayedCallback *dc, *dc_tmp;
    SIMPLEQ_FOREACH_SAFE(dc, &server->delayedCallbacks, next, dc_tmp) {
        SIMPLEQ_REMOVE_HEAD(&server->delayedCallbacks, next);
        if(dc->callback)
            dc->callback(server, dc->data);
        UA_free(dc);
    }
#ifdef UA_ENABLE_MULTITHREADING
    server->delayedCallbacks_checkpoint = NULL;
#endif
}

/**
 * Main Server Loop
 * ----------------
 * Start: Spin up the workers and the network layer and sample the server's
 *        start time.
 * Iterate: Process repeated callbacks and events in the network layer.
 *          This part can be driven from an external main-loop in an
 *          event-driven single-threaded architecture.
 * Stop: Stop workers, finish all callbacks, stop the network layer,
 *       clean up */

UA_StatusCode
UA_Server_run_startup(UA_Server *server) {
    UA_Variant var;
    UA_StatusCode result = UA_STATUSCODE_GOOD;
	
	/* At least one endpoint has to be configured */
    if(server->config.endpointsSize == 0) {
        UA_LOG_WARNING(server->config.logger, UA_LOGCATEGORY_SERVER,
                       "There has to be at least one endpoint.");
    }

    /* Sample the start time and set it to the Server object */
    server->startTime = UA_DateTime_now();
    UA_Variant_init(&var);
    UA_Variant_setScalar(&var, &server->startTime, &UA_TYPES[UA_TYPES_DATETIME]);
    UA_Server_writeValue(server,
                         UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER_SERVERSTATUS_STARTTIME),
                         var);

    /* Start the networklayers */
    for(size_t i = 0; i < server->config.networkLayersSize; ++i) {
        UA_ServerNetworkLayer *nl = &server->config.networkLayers[i];
        result |= nl->start(nl, &server->config.customHostname);
    }

    /* Spin up the worker threads */
#ifdef UA_ENABLE_MULTITHREADING
    UA_LOG_INFO(server->config.logger, UA_LOGCATEGORY_SERVER,
                "Spinning up %u worker thread(s)", server->config.nThreads);
    pthread_mutex_init(&server->dispatchQueue_accessMutex, NULL);
    pthread_cond_init(&server->dispatchQueue_condition, NULL);
    pthread_mutex_init(&server->dispatchQueue_conditionMutex, NULL);
    server->workers = (UA_Worker*)UA_malloc(server->config.nThreads * sizeof(UA_Worker));
    if(!server->workers)
        return UA_STATUSCODE_BADOUTOFMEMORY;
    for(size_t i = 0; i < server->config.nThreads; ++i) {
        UA_Worker *worker = &server->workers[i];
        worker->server = server;
        worker->counter = 0;
        worker->running = true;
        pthread_create(&worker->thr, NULL, (void* (*)(void*))workerLoop, worker);
    }
#endif

    /* Start the multicast discovery server */
#ifdef UA_ENABLE_DISCOVERY_MULTICAST
    if(server->config.applicationDescription.applicationType ==
       UA_APPLICATIONTYPE_DISCOVERYSERVER)
        startMulticastDiscoveryServer(server);
#endif

    return result;
}

UA_UInt16
UA_Server_run_iterate(UA_Server *server, UA_Boolean waitInternal) {
    /* Process repeated work */
    UA_DateTime now = UA_DateTime_nowMonotonic();
    UA_DateTime nextRepeated = UA_Timer_process(&server->timer, now, (UA_TimerDispatchCallback)executeCallback, server);
    UA_DateTime latest = now + (UA_MAXTIMEOUT * UA_DATETIME_MSEC);
    if(nextRepeated > latest)
        nextRepeated = latest;

    UA_UInt16 timeout = 0;

    /* round always to upper value to avoid timeout to be set to 0
    * if(nextRepeated - now) < (UA_DATETIME_MSEC/2) */
    if(waitInternal)
        timeout = (UA_UInt16)(((nextRepeated - now) + (UA_DATETIME_MSEC - 1)) / UA_DATETIME_MSEC);

    /* Listen on the networklayer */
    for(size_t i = 0; i < server->config.networkLayersSize; ++i) {
        UA_ServerNetworkLayer *nl = &server->config.networkLayers[i];
        nl->listen(nl, server, timeout);
    }

#if defined(UA_ENABLE_DISCOVERY_MULTICAST) && !defined(UA_ENABLE_MULTITHREADING)
    if(server->config.applicationDescription.applicationType ==
       UA_APPLICATIONTYPE_DISCOVERYSERVER) {
        // TODO multicastNextRepeat does not consider new input data (requests)
        // on the socket. It will be handled on the next call. if needed, we
        // need to use select with timeout on the multicast socket
        // server->mdnsSocket (see example in mdnsd library) on higher level.
        UA_DateTime multicastNextRepeat = 0;
        UA_StatusCode hasNext =
            iterateMulticastDiscoveryServer(server, &multicastNextRepeat, UA_TRUE);
        if(hasNext == UA_STATUSCODE_GOOD && multicastNextRepeat < nextRepeated)
            nextRepeated = multicastNextRepeat;
    }
#endif

#ifndef UA_ENABLE_MULTITHREADING
    UA_Server_cleanupDelayedCallbacks(server);
#endif

    now = UA_DateTime_nowMonotonic();
    timeout = 0;
    if(nextRepeated > now)
        timeout = (UA_UInt16)((nextRepeated - now) / UA_DATETIME_MSEC);
    return timeout;
}

UA_StatusCode
UA_Server_run_shutdown(UA_Server *server) {
    /* Stop the netowrk layer */
    for(size_t i = 0; i < server->config.networkLayersSize; ++i) {
        UA_ServerNetworkLayer *nl = &server->config.networkLayers[i];
        nl->stop(nl, server);
    }

#ifdef UA_ENABLE_MULTITHREADING
    /* Shut down the workers */
    if(server->workers) {
        UA_LOG_INFO(server->config.logger, UA_LOGCATEGORY_SERVER,
                    "Shutting down %u worker thread(s)",
                    server->config.nThreads);
        for(size_t i = 0; i < server->config.nThreads; ++i)
            server->workers[i].running = false;
        pthread_cond_broadcast(&server->dispatchQueue_condition);
        for(size_t i = 0; i < server->config.nThreads; ++i)
            pthread_join(server->workers[i].thr, NULL);
        UA_free(server->workers);
        server->workers = NULL;
    }
#endif

    /* Dispatch all delayed callbacks */
    UA_Server_cleanupDelayedCallbacks(server);

#ifdef UA_ENABLE_MULTITHREADING
    /* Execute the remaining callbacks in the dispatch queue. Also executes
     * delayed callbacks. */
    UA_Server_cleanupDispatchQueue(server);
#endif

#ifdef UA_ENABLE_DISCOVERY_MULTICAST
    /* Stop multicast discovery */
    if(server->config.applicationDescription.applicationType ==
       UA_APPLICATIONTYPE_DISCOVERYSERVER)
        stopMulticastDiscoveryServer(server);
#endif

    return UA_STATUSCODE_GOOD;
}

UA_StatusCode
UA_Server_run(UA_Server *server, volatile UA_Boolean *running) {
    UA_StatusCode retval = UA_Server_run_startup(server);
    if(retval != UA_STATUSCODE_GOOD)
        return retval;
#ifdef UA_ENABLE_VALGRIND_INTERACTIVE
    size_t loopCount = 0;
#endif
    while(*running) {
#ifdef UA_ENABLE_VALGRIND_INTERACTIVE
        if(loopCount == 0) {
            VALGRIND_DO_LEAK_CHECK;
        }
        ++loopCount;
        loopCount %= UA_VALGRIND_INTERACTIVE_INTERVAL;
#endif
        UA_Server_run_iterate(server, true);
    }
    return UA_Server_run_shutdown(server);
}
