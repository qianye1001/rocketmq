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

package org.apache.rocketmq.proxy.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;

public class BatchChangeInvisibleTimeUtils {
    public static List<String> batchKey(ReceiptHandle handle, String group, String topic) {
        return Arrays.asList(group, topic, handle.getBrokerName(), handle.getRealTopic(topic, group));
    }

    public static Collection<List<ReceiptHandleMessage>> groupByBroker(List<ReceiptHandleMessage> messages,
        String group, String topic) {
        Map<List<String>, List<ReceiptHandleMessage>> batches = new LinkedHashMap<>();
        for (ReceiptHandleMessage message : messages) {
            batches.computeIfAbsent(batchKey(message.getReceiptHandle(), group, topic), ignored -> new ArrayList<>())
                .add(message);
        }
        return batches.values();
    }

    public static void validateBatch(List<ReceiptHandleMessage> messages, String group, String topic) {
        if (messages.isEmpty()) {
            return;
        }
        List<String> key = batchKey(messages.get(0).getReceiptHandle(), group, topic);
        for (ReceiptHandleMessage message : messages) {
            if (!key.equals(batchKey(message.getReceiptHandle(), group, topic))) {
                throw new IllegalArgumentException("batch change invisible time requires the same broker and real topic");
            }
        }
    }
}
