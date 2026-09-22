/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.common;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A single broker's renewal batch. Results correspond to handles in input order. */
public class RenewEvent {
    private final ReceiptHandleGroupKey key;
    private final List<MessageReceiptHandle> messageReceiptHandleList;
    private final List<Long> renewTimeList;
    private final EventType eventType;
    private final CompletableFuture<List<BatchChangeInvisibleTimeResult>> future = new CompletableFuture<>();

    public enum EventType {
        RENEW,
        STOP_RENEW,
        CLEAR_GROUP
    }

    public RenewEvent(ReceiptHandleGroupKey key, MessageReceiptHandle messageReceiptHandle, long renewTime,
        EventType eventType) {
        this(key, Collections.singletonList(messageReceiptHandle), Collections.singletonList(renewTime), eventType);
    }

    public RenewEvent(ReceiptHandleGroupKey key, List<MessageReceiptHandle> messageReceiptHandleList,
        List<Long> renewTimeList, EventType eventType) {
        this.key = key;
        this.messageReceiptHandleList = messageReceiptHandleList;
        this.renewTimeList = renewTimeList;
        this.eventType = eventType;
    }

    public ReceiptHandleGroupKey getKey() {
        return key;
    }

    public List<MessageReceiptHandle> getMessageReceiptHandleList() {
        return messageReceiptHandleList;
    }

    public List<Long> getRenewTimeList() {
        return renewTimeList;
    }

    public EventType getEventType() {
        return eventType;
    }

    public CompletableFuture<List<BatchChangeInvisibleTimeResult>> getFuture() {
        return future;
    }
}
