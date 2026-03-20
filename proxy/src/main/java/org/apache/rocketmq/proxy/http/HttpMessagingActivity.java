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
package org.apache.rocketmq.proxy.http;

import com.alibaba.fastjson2.JSONObject;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;

/**
 * HTTP messaging activity interface, analogous to GrpcMessagingActivity.
 * Each method accepts parsed JSON parameters and returns a JSON response asynchronously.
 */
public interface HttpMessagingActivity {

    /**
     * Send a message to a topic.
     *
     * @param ctx   proxy context
     * @param topic destination topic
     * @param body  request JSON body containing message fields
     * @return future of JSON response
     */
    CompletableFuture<JSONObject> sendMessage(ProxyContext ctx, String topic, JSONObject body);

    /**
     * Receive (pop) messages from a topic.
     *
     * @param ctx            proxy context
     * @param topic          source topic
     * @param consumerGroup  consumer group
     * @param maxMsgNums     max number of messages to receive
     * @param invisibleTime  invisible time in millis
     * @param waitTimeMillis long polling wait time in millis
     * @return future of JSON response containing messages
     */
    CompletableFuture<JSONObject> receiveMessage(ProxyContext ctx, String topic, String consumerGroup,
        int maxMsgNums, long invisibleTime, long waitTimeMillis);

    /**
     * Acknowledge (delete) a message.
     *
     * @param ctx           proxy context
     * @param topic         topic name
     * @param consumerGroup consumer group
     * @param receiptHandle receipt handle string
     * @return future of JSON response
     */
    CompletableFuture<JSONObject> ackMessage(ProxyContext ctx, String topic, String consumerGroup,
        String receiptHandle);

    /**
     * Change the invisible time (visibility timeout) of a message.
     *
     * @param ctx           proxy context
     * @param topic         topic name
     * @param consumerGroup consumer group
     * @param receiptHandle receipt handle string
     * @param invisibleTime new invisible time in millis
     * @return future of JSON response
     */
    CompletableFuture<JSONObject> changeInvisibleTime(ProxyContext ctx, String topic, String consumerGroup,
        String receiptHandle, long invisibleTime);
}
