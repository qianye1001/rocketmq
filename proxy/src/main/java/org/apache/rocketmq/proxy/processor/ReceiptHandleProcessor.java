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

package org.apache.rocketmq.proxy.processor;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.state.StateEventListener;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.BatchChangeInvisibleTimeResult;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.RenewEvent;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.receipt.DefaultReceiptHandleManager;

public class ReceiptHandleProcessor extends AbstractProcessor {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected DefaultReceiptHandleManager receiptHandleManager;

    public ReceiptHandleProcessor(MessagingProcessor messagingProcessor, ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);
        StateEventListener<RenewEvent> eventListener = event -> {
            ProxyContext context = createContext(event.getEventType().name())
                .setChannel(event.getKey().getChannel());
            changeInvisibleTime(context, event);
        };
        this.receiptHandleManager = new DefaultReceiptHandleManager(serviceManager.getMetadataService(), serviceManager.getConsumerManager(), eventListener);
        this.appendStartAndShutdown(receiptHandleManager);
    }

    protected void changeInvisibleTime(ProxyContext context, RenewEvent event) {
        try {
            List<MessageReceiptHandle> messages = event.getMessageReceiptHandleList();
            MessageReceiptHandle first = messages.get(0);
            List<ReceiptHandleMessage> handles = new ArrayList<>(messages.size());
            for (int i = 0; i < messages.size(); i++) {
                MessageReceiptHandle message = messages.get(i);
                handles.add(new ReceiptHandleMessage(ReceiptHandle.decode(message.getReceiptHandleStr()),
                    message.getMessageId(), message.getLiteTopic(), event.getRenewTimeList().get(i)));
            }
            if (handles.size() == 1) {
                ReceiptHandleMessage handle = handles.get(0);
                messagingProcessor.changeInvisibleTime(context, handle.getReceiptHandle(), handle.getMessageId(),
                    first.getGroup(), first.getTopic(), handle.getInvisibleTime(), handle.getLiteTopic())
                    .whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            event.getFuture().completeExceptionally(throwable);
                        } else {
                            event.getFuture().complete(Collections.singletonList(new BatchChangeInvisibleTimeResult(handle, result)));
                        }
                    });
            } else {
                messagingProcessor.batchChangeInvisibleTime(context, handles, first.getGroup(), first.getTopic(),
                    handles.get(0).getInvisibleTime(), MessagingProcessor.DEFAULT_TIMEOUT_MILLS, false)
                    .whenComplete((results, throwable) -> {
                        if (throwable != null) {
                            event.getFuture().completeExceptionally(throwable);
                        } else {
                            event.getFuture().complete(results);
                        }
                    });
            }
        } catch (Throwable t) {
            event.getFuture().completeExceptionally(t);
        }
    }

    protected ProxyContext createContext(String actionName) {
        return ProxyContext.createForInner(this.getClass().getSimpleName() + actionName);
    }

    public void addReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID, MessageReceiptHandle messageReceiptHandle) {
        receiptHandleManager.addReceiptHandle(ctx, channel, group, msgID, messageReceiptHandle);
    }

    public MessageReceiptHandle removeReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID, String receiptHandle) {
        return receiptHandleManager.removeReceiptHandle(ctx, channel, group, msgID, receiptHandle);
    }

    public int getUnackedMessageCount(ProxyContext ctx, Channel channel, String group) {
        return receiptHandleManager.getUnackedMessageCount(ctx, channel, group);
    }

}
