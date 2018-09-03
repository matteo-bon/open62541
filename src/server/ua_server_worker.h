/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 *    Copyright 2018 (c) Fraunhofer IOSB (Author: Julius Pfrommer)
 */

#ifndef UA_SERVER_WORKER_H_
#define UA_SERVER_WORKER_H_

#include "ua_server.h"
#include "../../deps/queue.h"

_UA_BEGIN_DECLS

typedef struct UA_DelayedCallback {
    SIMPLEQ_ENTRY(UA_DelayedCallback) next;
    UA_ServerCallback callback;
    void *data;
} UA_DelayedCallback;

/* Delayed callbacks are executed when all previously dispatched callbacks are
 * finished */
void UA_Server_delayedCallback(UA_Server *server, UA_DelayedCallback *cb);

void UA_Server_cleanupDelayedCallbacks(UA_Server *server);

#ifdef UA_ENABLE_MULTITHREADING
#include <pthread.h>

typedef struct {
    UA_Server *server;
    pthread_t thr;
    volatile UA_Boolean running;
    UA_UInt32 counter;
    UA_UInt32 checkpointCounter; /* Counter when the last checkpoint was made
                                  * for the delayed callbacks */

    /* separate cache lines */
    char padding[64 - sizeof(void*) - sizeof(pthread_t) -
                 sizeof(UA_UInt32) - sizeof(UA_Boolean)];
} UA_Worker;

void UA_Server_cleanupDispatchQueue(UA_Server *server);

#endif

_UA_END_DECLS

#endif /* UA_SERVER_WORKER_H_ */

