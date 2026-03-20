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

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class DefaultHttpMessagingActivity implements HttpMessagingActivity {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final MessagingProcessor messagingProcessor;

    public DefaultHttpMessagingActivity(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
    }

    @Override
    public CompletableFuture<JSONObject> sendMessage(ProxyContext ctx, String topic, JSONObject body) {
        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        try {
            String messageBody = body.getString("body");
            if (StringUtils.isBlank(messageBody)) {
                throw new IllegalArgumentException("message body cannot be empty");
            }

            String tags = body.getString("tags");
            String keys = body.getString("keys");
            String producerGroup = body.getString("producerGroup");
            if (StringUtils.isBlank(producerGroup)) {
                producerGroup = "DEFAULT_HTTP_PRODUCER";
            }

            Map<String, String> properties = null;
            JSONObject propsObj = body.getJSONObject("properties");
            if (propsObj != null) {
                properties = propsObj.toJavaObject(Map.class);
            }

            Message message = new Message();
            message.setTopic(topic);
            message.setBody(messageBody.getBytes(StandardCharsets.UTF_8));
            if (StringUtils.isNotBlank(tags)) {
                message.setTags(tags);
            }
            if (StringUtils.isNotBlank(keys)) {
                message.setKeys(keys);
            }
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    message.putUserProperty(entry.getKey(), entry.getValue());
                }
            }

            String messageId = UUID.randomUUID().toString();
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);

            List<Message> messageList = new ArrayList<>();
            messageList.add(message);

            final String group = producerGroup;
            messagingProcessor.sendMessage(
                ctx,
                new HttpQueueSelector(),
                group,
                0,
                messageList
            ).thenAccept(results -> {
                JSONObject result = new JSONObject();
                if (!results.isEmpty()) {
                    SendResult sendResult = results.get(0);
                    result.put("messageId", sendResult.getMsgId());
                    result.put("sendStatus", sendResult.getSendStatus().name());
                    if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                        result.put("queueOffset", sendResult.getQueueOffset());
                    }
                }
                future.complete(result);
            }).exceptionally(t -> {
                future.completeExceptionally(t);
                return null;
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public CompletableFuture<JSONObject> receiveMessage(ProxyContext ctx, String topic, String consumerGroup,
        int maxMsgNums, long invisibleTime, long waitTimeMillis) {
        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(consumerGroup)) {
                throw new IllegalArgumentException("consumerGroup cannot be empty");
            }

            ProxyConfig config = ConfigurationManager.getProxyConfig();
            if (invisibleTime <= 0) {
                invisibleTime = config.getDefaultInvisibleTimeMills();
            }
            if (maxMsgNums <= 0) {
                maxMsgNums = 1;
            }
            if (maxMsgNums > 32) {
                maxMsgNums = 32;
            }
            if (waitTimeMillis <= 0) {
                waitTimeMillis = config.getHttpLongPollingTimeoutMillis();
            }

            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, "*");

            long timeoutMillis = waitTimeMillis + 5000;

            messagingProcessor.popMessage(
                ctx,
                new HttpQueueSelector(),
                consumerGroup,
                topic,
                maxMsgNums,
                invisibleTime,
                waitTimeMillis,
                ConsumeInitMode.MAX,
                subscriptionData,
                false,
                null,
                null,
                timeoutMillis
            ).thenAccept(popResult -> {
                JSONObject result = convertPopResult(popResult);
                future.complete(result);
            }).exceptionally(t -> {
                future.completeExceptionally(t);
                return null;
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public CompletableFuture<JSONObject> ackMessage(ProxyContext ctx, String topic, String consumerGroup,
        String receiptHandleStr) {
        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(receiptHandleStr)) {
                throw new IllegalArgumentException("receiptHandle cannot be empty");
            }
            if (StringUtils.isBlank(consumerGroup)) {
                throw new IllegalArgumentException("consumerGroup cannot be empty");
            }

            ReceiptHandle receiptHandle = ReceiptHandle.decode(receiptHandleStr);

            messagingProcessor.ackMessage(
                ctx,
                receiptHandle,
                null,
                consumerGroup,
                topic
            ).thenAccept(ackResult -> {
                JSONObject result = new JSONObject();
                result.put("status", ackResult.getStatus().name());
                future.complete(result);
            }).exceptionally(t -> {
                future.completeExceptionally(t);
                return null;
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public CompletableFuture<JSONObject> changeInvisibleTime(ProxyContext ctx, String topic, String consumerGroup,
        String receiptHandleStr, long invisibleTime) {
        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(receiptHandleStr)) {
                throw new IllegalArgumentException("receiptHandle cannot be empty");
            }
            if (StringUtils.isBlank(consumerGroup)) {
                throw new IllegalArgumentException("consumerGroup cannot be empty");
            }
            if (invisibleTime <= 0) {
                throw new IllegalArgumentException("invisibleTime must be positive");
            }

            ReceiptHandle receiptHandle = ReceiptHandle.decode(receiptHandleStr);

            messagingProcessor.changeInvisibleTime(
                ctx,
                receiptHandle,
                null,
                consumerGroup,
                topic,
                invisibleTime
            ).thenAccept(ackResult -> {
                JSONObject result = new JSONObject();
                result.put("status", ackResult.getStatus().name());
                future.complete(result);
            }).exceptionally(t -> {
                future.completeExceptionally(t);
                return null;
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private JSONObject convertPopResult(PopResult popResult) {
        JSONObject result = new JSONObject();
        result.put("popStatus", popResult.getPopStatus().name());

        if (popResult.getPopStatus() == PopStatus.FOUND) {
            JSONArray messages = new JSONArray();
            for (MessageExt msgExt : popResult.getMsgFoundList()) {
                JSONObject msgJson = new JSONObject();
                msgJson.put("messageId", msgExt.getMsgId());
                msgJson.put("topic", msgExt.getTopic());
                msgJson.put("body", new String(msgExt.getBody(), StandardCharsets.UTF_8));
                msgJson.put("bornTimestamp", msgExt.getBornTimestamp());
                msgJson.put("storeTimestamp", msgExt.getStoreTimestamp());
                msgJson.put("queueId", msgExt.getQueueId());
                msgJson.put("queueOffset", msgExt.getQueueOffset());
                msgJson.put("reconsumeTimes", msgExt.getReconsumeTimes());

                String tags = msgExt.getTags();
                if (tags != null) {
                    msgJson.put("tags", tags);
                }

                String keys = msgExt.getKeys();
                if (keys != null) {
                    msgJson.put("keys", keys);
                }

                // receipt handle for ack
                String receiptHandle = msgExt.getProperty(MessageConst.PROPERTY_POP_CK);
                if (receiptHandle != null) {
                    msgJson.put("receiptHandle", receiptHandle);
                }

                // user properties
                Map<String, String> properties = msgExt.getProperties();
                if (properties != null && !properties.isEmpty()) {
                    JSONObject propsJson = new JSONObject();
                    for (Map.Entry<String, String> entry : properties.entrySet()) {
                        if (!MessageConst.STRING_HASH_SET.contains(entry.getKey())) {
                            propsJson.put(entry.getKey(), entry.getValue());
                        }
                    }
                    if (!propsJson.isEmpty()) {
                        msgJson.put("properties", propsJson);
                    }
                }

                messages.add(msgJson);
            }
            result.put("messages", messages);
        } else {
            result.put("messages", new JSONArray());
        }

        return result;
    }

    private static class HttpQueueSelector implements QueueSelector {
        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            try {
                return messageQueueView.getWriteSelector().selectOneByPipeline(false);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
