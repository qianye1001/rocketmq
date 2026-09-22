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

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.local.LocalChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroupKey;
import org.apache.rocketmq.proxy.common.RenewEvent;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

public class ReceiptHandleBatchCompletionTest extends BaseProcessorTest {
    private final List<List<ReceiptHandleMessage>> requests = new ArrayList<>();
    private final List<CompletableFuture<List<AckResult>>> responses = new ArrayList<>();
    private ExecutorService executor;
    private ReceiptHandleProcessor receiptProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        executor = MoreExecutors.newDirectExecutorService();
        ConsumerProcessor consumerProcessor = new ConsumerProcessor(messagingProcessor, serviceManager, executor);
        receiptProcessor = new ReceiptHandleProcessor(messagingProcessor, serviceManager);
        doAnswer(invocation -> consumerProcessor.batchChangeInvisibleTime(
            invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2),
            invocation.getArgument(3), invocation.getArgument(4), invocation.getArgument(5), invocation.getArgument(6)))
            .when(messagingProcessor).batchChangeInvisibleTime(
                any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        doAnswer(invocation -> {
            requests.add(new ArrayList<>(invocation.getArgument(1)));
            CompletableFuture<List<AckResult>> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        }).when(messageService).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
    }

    @After
    public void after() {
        try {
            receiptProcessor.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            executor.shutdownNow();
            super.after();
        }
    }

    @Test
    public void testFastBrokerReleasesHandlesBeforeSlowBrokerCompletes() {
        RenewEvent event = event(
            message("fast", ReceiptHandle.NORMAL_TOPIC, 0), message("slow", ReceiptHandle.NORMAL_TOPIC, 1),
            message("fast", ReceiptHandle.NORMAL_TOPIC, 2), message("slow", ReceiptHandle.NORMAL_TOPIC, 3));
        ReceiptHandleGroup group = new ReceiptHandleGroup();
        for (int i = 0; i < event.getMessageReceiptHandleList().size(); i++) {
            MessageReceiptHandle message = event.getMessageReceiptHandleList().get(i);
            CompletableFuture<AckResult> ackFuture = event.getFutureList().get(i);
            group.put(message.getMessageId(), message);
            group.computeIfPresent(message.getMessageId(), message.getOriginalReceiptHandleStr(), current ->
                ackFuture.thenApply(result -> {
                    current.updateReceiptHandle(result.getExtraInfo());
                    return current;
                }));
        }

        receiptProcessor.batchChangeInvisibleTime(createContext(), event);
        assertEquals(2, requests.size());
        assertFalse(event.getFutureList().get(0).isDone());
        assertFalse(event.getFutureList().get(1).isDone());
        int fastRequest = requestIndex("fast", ReceiptHandle.NORMAL_TOPIC);
        List<AckResult> fastResults = successResults(requests.get(fastRequest));
        responses.get(fastRequest).complete(fastResults);

        assertTrue(event.getFutureList().get(0).isDone());
        assertTrue(event.getFutureList().get(2).isDone());
        assertFalse(event.getFutureList().get(1).isDone());
        assertFalse(event.getFutureList().get(3).isDone());
        MessageReceiptHandle fastMessage = event.getMessageReceiptHandleList().get(0);
        MessageReceiptHandle removed = group.remove(fastMessage.getMessageId(), fastMessage.getOriginalReceiptHandleStr());
        assertEquals(fastResults.get(0).getExtraInfo() + MessageConst.KEY_SEPARATOR + 100, removed.getReceiptHandleStr());
        completeSuccess(requestIndex("slow", ReceiptHandle.NORMAL_TOPIC));
    }

    @Test
    public void testCompletedChunkReleasesHandlesBeforeNextChunkCompletes() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        RenewEvent event = event(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 1), message("broker", ReceiptHandle.NORMAL_TOPIC, 2),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 3));
        receiptProcessor.batchChangeInvisibleTime(createContext(), event);
        assertEquals(1, requests.size());
        assertFalse(event.getFutureList().get(0).isDone());
        completeSuccess(0);
        assertEquals(2, requests.size());
        assertTrue(event.getFutureList().get(0).isDone());
        assertTrue(event.getFutureList().get(1).isDone());
        assertFalse(event.getFutureList().get(2).isDone());
        assertFalse(event.getFutureList().get(3).isDone());
        assertEquals(30002L, requests.get(1).get(0).getInvisibleTime());
        completeSuccess(1);
        assertTrue(event.getFutureList().get(2).isDone());
        assertTrue(event.getFutureList().get(3).isDone());
    }

    @Test
    public void testFinalSingleHandleChunkCompletesSeparately() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        CompletableFuture<AckResult> lastResponse = new CompletableFuture<>();
        doAnswer(invocation -> lastResponse).when(messageService).changeInvisibleTime(
            any(), any(), anyString(), any(), anyLong());
        RenewEvent event = event(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 1), message("broker", ReceiptHandle.NORMAL_TOPIC, 2));
        receiptProcessor.batchChangeInvisibleTime(createContext(), event);
        completeSuccess(0);
        assertTrue(event.getFutureList().get(0).isDone());
        assertTrue(event.getFutureList().get(1).isDone());
        assertFalse(event.getFutureList().get(2).isDone());
        AckResult result = new AckResult();
        result.setStatus(AckStatus.OK);
        lastResponse.complete(result);
        assertEquals(AckStatus.OK, event.getFutureList().get(2).join().getStatus());
    }

    @Test
    public void testRetryTopicCompletesIndependentlyOnSameBroker() {
        RenewEvent event = event(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.RETRY_TOPIC_V2, 1), message("broker", ReceiptHandle.NORMAL_TOPIC, 2),
            message("broker", ReceiptHandle.RETRY_TOPIC_V2, 3));
        receiptProcessor.batchChangeInvisibleTime(createContext(), event);
        assertEquals(2, requests.size());
        completeSuccess(requestIndex("broker", ReceiptHandle.RETRY_TOPIC_V2));
        assertTrue(event.getFutureList().get(1).isDone());
        assertTrue(event.getFutureList().get(3).isDone());
        assertFalse(event.getFutureList().get(0).isDone());
        assertFalse(event.getFutureList().get(2).isDone());
        completeSuccess(requestIndex("broker", ReceiptHandle.NORMAL_TOPIC));
    }

    @Test
    public void testFailedChunkDoesNotPreventLaterChunkCompletion() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        RenewEvent event = event(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 1), message("broker", ReceiptHandle.NORMAL_TOPIC, 2),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 3));
        receiptProcessor.batchChangeInvisibleTime(createContext(), event);
        responses.get(0).completeExceptionally(new TimeoutException("broker response lost"));
        assertEquals(2, requests.size());
        assertTrue(event.getFutureList().get(0).isCompletedExceptionally());
        assertTrue(event.getFutureList().get(1).isCompletedExceptionally());
        assertFalse(event.getFutureList().get(2).isDone());
        completeSuccess(1);
        assertEquals(AckStatus.OK, event.getFutureList().get(2).join().getStatus());
        assertEquals(AckStatus.OK, event.getFutureList().get(3).join().getStatus());
        assertEquals(2, requests.size());
    }

    @Test
    public void testSynchronousSubmissionFailureDoesNotLeaveLaterChunksPending() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        doAnswer(invocation -> {
            throw new IllegalStateException("request could not be submitted");
        }).when(messagingProcessor).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        RenewEvent event = event(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 1), message("broker", ReceiptHandle.NORMAL_TOPIC, 2));
        receiptProcessor.batchChangeInvisibleTime(createContext(), event);
        for (CompletableFuture<AckResult> future : event.getFutureList()) {
            assertTrue(future.isCompletedExceptionally());
        }
    }

    private MessageReceiptHandle message(String broker, String topicType, int index) {
        String handle = ReceiptHandle.builder().startOffset(0).retrieveTime(System.currentTimeMillis())
            .invisibleTime(60000).reviveQueueId(1).topicType(topicType).brokerName(broker)
            .queueId(0).offset(index).commitLogOffset(100 + index).build().encode();
        return new MessageReceiptHandle("group", "topic", 0, handle, "msg-" + index, index, 0);
    }

    private RenewEvent event(MessageReceiptHandle... messages) {
        List<Long> times = new ArrayList<>();
        List<CompletableFuture<AckResult>> futures = new ArrayList<>();
        for (int i = 0; i < messages.length; i++) {
            times.add(30000L + i);
            futures.add(new CompletableFuture<>());
        }
        return new RenewEvent(new ReceiptHandleGroupKey(new LocalChannel(), "group"), Arrays.asList(messages),
            times, RenewEvent.EventType.RENEW, futures);
    }

    private int requestIndex(String broker, String topicType) {
        for (int i = 0; i < requests.size(); i++) {
            ReceiptHandle handle = requests.get(i).get(0).getReceiptHandle();
            if (broker.equals(handle.getBrokerName()) && topicType.equals(handle.getTopicType())) {
                return i;
            }
        }
        throw new AssertionError("request not sent for " + broker + "/" + topicType);
    }

    private List<AckResult> successResults(List<ReceiptHandleMessage> messages) {
        List<AckResult> results = new ArrayList<>();
        for (ReceiptHandleMessage message : messages) {
            ReceiptHandle old = message.getReceiptHandle();
            AckResult result = new AckResult();
            result.setStatus(AckStatus.OK);
            result.setExtraInfo(ExtraInfoUtil.buildExtraInfo(old.getStartOffset(), System.currentTimeMillis(),
                message.getInvisibleTime(), old.getReviveQueueId(), old.getRealTopic("topic", "group"),
                old.getBrokerName(), old.getQueueId()) + MessageConst.KEY_SEPARATOR + old.getOffset());
            results.add(result);
        }
        return results;
    }

    private void completeSuccess(int index) {
        responses.get(index).complete(successResults(requests.get(index)));
    }
}
