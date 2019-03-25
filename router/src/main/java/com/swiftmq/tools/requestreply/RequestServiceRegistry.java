/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.tools.requestreply;

import com.swiftmq.tools.collection.ArrayListTool;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The RequestServiceRegistry is responsible to register RequestServices for dispatchIds
 * and dispatches incoming Requests to RequestServices. Is there are no RequestService
 * registered for this dispatchId, a Reply with an exception is send back.
 */
public class RequestServiceRegistry {
    ReplyHandler replyHandler;
    ArrayList dispatchList = new ArrayList();
    ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    int nextFree = -1;

    public void setReplyHandler(ReplyHandler replyHandler) {
        this.replyHandler = replyHandler;
    }

    public int getNumberServices() {
        rwl.readLock().lock();
        try {
            return dispatchList.size();
        } finally {
            rwl.readLock().unlock();
        }
    }

    public int getNextFreeDispatchId() {
        rwl.readLock().lock();
        try {
            if (nextFree == -1)
                nextFree = ArrayListTool.setFirstFreeOrExpand(dispatchList, null);
            return nextFree;
        } finally {
            rwl.readLock().unlock();
        }
    }

    public int addRequestService(RequestService requestService) {
        rwl.writeLock().lock();
        try {
            nextFree = -1;
            int idx = ArrayListTool.setFirstFreeOrExpand(dispatchList, requestService);
            return idx;
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public void removeRequestService(int dispatchId) {
        rwl.writeLock().lock();
        try {
            nextFree = -1;
            dispatchList.set(dispatchId, null);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public RequestService getRequestService(int dispatchId) {
        rwl.readLock().lock();
        try {
            return (RequestService) dispatchList.get(dispatchId);
        } finally {
            rwl.readLock().unlock();
        }
    }

    protected boolean isSendExceptionEnabled() {
        return true;
    }

    public void dispatch(Request request) {
        if (request.isReplyRequired())
            request.setReplyHandler(replyHandler);
        RequestService requestService = getRequestService(request.getDispatchId());
        if (requestService != null) {
            requestService.serviceRequest(request);
        } else {
            if (request.isReplyRequired() && isSendExceptionEnabled()) {
                Reply reply = request.createReply();
                reply.setOk(false);
                reply.setException(new TransportException("No requestService for request defined: " + request));
                reply.send();
            }
        }
    }
}

