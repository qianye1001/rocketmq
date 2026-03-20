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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultHttpMessagingActivityTest extends InitConfigTest {

    @Mock
    private MessagingProcessor messagingProcessor;

    private DefaultHttpMessagingActivity activity;
    private ProxyContext proxyContext;

    private static final String TEST_TOPIC = "TestTopic";
    private static final String TEST_GROUP = "TestGroup";

    @Before
    @Override
    public void before() throws Throwable {
        super.before();
        activity = new DefaultHttpMessagingActivity(messagingProcessor);
        proxyContext = ProxyContext.create()
            .setRequestId("test-req-id")
            .setRemoteAddress("127.0.0.1:9999")
            .setLocalAddress("127.0.0.1:8082");
    }

    // ─── sendMessage ────────────────────────────────────────────────────────────

    @Test
    public void testSendMessage_success() throws Exception {
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("msg-send-001");
        sendResult.setQueueOffset(42L);

        when(messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), anyList()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        JSONObject body = new JSONObject();
        body.put("body", "Hello RocketMQ");
        body.put("tags", "TagA");
        body.put("keys", "key-001");

        JSONObject result = activity.sendMessage(proxyContext, TEST_TOPIC, body).get();

        assertThat(result.getString("messageId")).isEqualTo("msg-send-001");
        assertThat(result.getString("sendStatus")).isEqualTo("SEND_OK");
        assertThat(result.getLong("queueOffset")).isEqualTo(42L);
    }

    @Test
    public void testSendMessage_emptyBody_throwsIllegalArgument() {
        JSONObject body = new JSONObject();
        body.put("body", "");

        CompletableFuture<JSONObject> future = activity.sendMessage(proxyContext, TEST_TOPIC, body);

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("message body cannot be empty");
    }

    @Test
    public void testSendMessage_nullBody_throwsIllegalArgument() {
        JSONObject body = new JSONObject();
        // no "body" key at all

        CompletableFuture<JSONObject> future = activity.sendMessage(proxyContext, TEST_TOPIC, body);

        assertThat(future.isCompletedExceptionally()).isTrue();
    }

    @Test
    public void testSendMessage_usesDefaultProducerGroupWhenNotProvided() throws Exception {
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("msg-002");

        ArgumentCaptor<String> groupCaptor = ArgumentCaptor.forClass(String.class);
        when(messagingProcessor.sendMessage(any(), any(), groupCaptor.capture(), anyInt(), anyList()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        JSONObject body = new JSONObject();
        body.put("body", "test message");

        activity.sendMessage(proxyContext, TEST_TOPIC, body).get();

        assertThat(groupCaptor.getValue()).isEqualTo("DEFAULT_HTTP_PRODUCER");
    }

    @Test
    public void testSendMessage_usesCustomProducerGroup() throws Exception {
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("msg-003");

        ArgumentCaptor<String> groupCaptor = ArgumentCaptor.forClass(String.class);
        when(messagingProcessor.sendMessage(any(), any(), groupCaptor.capture(), anyInt(), anyList()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        JSONObject body = new JSONObject();
        body.put("body", "test message");
        body.put("producerGroup", "MyProducerGroup");

        activity.sendMessage(proxyContext, TEST_TOPIC, body).get();

        assertThat(groupCaptor.getValue()).isEqualTo("MyProducerGroup");
    }

    @Test
    public void testSendMessage_withUserProperties() throws Exception {
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("msg-004");

        when(messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), anyList()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        JSONObject props = new JSONObject();
        props.put("customKey", "customValue");

        JSONObject body = new JSONObject();
        body.put("body", "test message");
        body.put("properties", props);

        JSONObject result = activity.sendMessage(proxyContext, TEST_TOPIC, body).get();

        assertThat(result.getString("sendStatus")).isEqualTo("SEND_OK");
    }

    // ─── receiveMessage ─────────────────────────────────────────────────────────

    @Test
    public void testReceiveMessage_success_withMessages() throws Exception {
        MessageExt msgExt = new MessageExt();
        msgExt.setMsgId("pop-msg-001");
        msgExt.setTopic(TEST_TOPIC);
        msgExt.setBody("Hello Consumer".getBytes(StandardCharsets.UTF_8));
        msgExt.setBornTimestamp(System.currentTimeMillis());
        msgExt.setStoreTimestamp(System.currentTimeMillis());
        msgExt.setQueueId(0);
        msgExt.setQueueOffset(10L);
        msgExt.setTags("TagB");
        // Simulate receipt handle property
        msgExt.putUserProperty("PROPERTY_POP_CK", "receipt-handle-xyz");

        PopResult popResult = new PopResult(PopStatus.FOUND, Collections.singletonList(msgExt));

        when(messagingProcessor.popMessage(any(), any(), anyString(), anyString(), anyInt(),
            anyLong(), anyLong(), anyInt(), any(), anyBoolean(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));

        JSONObject result = activity.receiveMessage(proxyContext, TEST_TOPIC, TEST_GROUP,
            1, 30000L, 5000L).get();

        assertThat(result.getString("popStatus")).isEqualTo("FOUND");
        assertThat(result.getJSONArray("messages")).isNotNull();
        assertThat(result.getJSONArray("messages")).hasSize(1);

        JSONObject msg = result.getJSONArray("messages").getJSONObject(0);
        assertThat(msg.getString("messageId")).isEqualTo("pop-msg-001");
        assertThat(msg.getString("body")).isEqualTo("Hello Consumer");
        assertThat(msg.getString("tags")).isEqualTo("TagB");
    }

    @Test
    public void testReceiveMessage_noNewMessage() throws Exception {
        PopResult popResult = new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList());

        when(messagingProcessor.popMessage(any(), any(), anyString(), anyString(), anyInt(),
            anyLong(), anyLong(), anyInt(), any(), anyBoolean(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));

        JSONObject result = activity.receiveMessage(proxyContext, TEST_TOPIC, TEST_GROUP,
            1, 30000L, 5000L).get();

        assertThat(result.getString("popStatus")).isEqualTo("NO_NEW_MSG");
        assertThat(result.getJSONArray("messages")).isEmpty();
    }

    @Test
    public void testReceiveMessage_emptyConsumerGroup_throwsIllegalArgument() {
        CompletableFuture<JSONObject> future = activity.receiveMessage(proxyContext, TEST_TOPIC,
            "", 1, 30000L, 5000L);

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("consumerGroup cannot be empty");
    }

    @Test
    public void testReceiveMessage_nullConsumerGroup_throwsIllegalArgument() {
        CompletableFuture<JSONObject> future = activity.receiveMessage(proxyContext, TEST_TOPIC,
            null, 1, 30000L, 5000L);

        assertThat(future.isCompletedExceptionally()).isTrue();
    }

    @Test
    public void testReceiveMessage_maxMsgNumsClampedTo32() throws Exception {
        PopResult popResult = new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList());

        ArgumentCaptor<Integer> maxMsgCaptor = ArgumentCaptor.forClass(Integer.class);
        when(messagingProcessor.popMessage(any(), any(), anyString(), anyString(),
            maxMsgCaptor.capture(), anyLong(), anyLong(), anyInt(), any(), anyBoolean(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));

        // Request 100 messages — should be clamped to 32
        activity.receiveMessage(proxyContext, TEST_TOPIC, TEST_GROUP, 100, 30000L, 5000L).get();

        assertThat(maxMsgCaptor.getValue()).isEqualTo(32);
    }

    // ─── ackMessage ─────────────────────────────────────────────────────────────

    @Test
    public void testAckMessage_success() throws Exception {
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);

        when(messagingProcessor.ackMessage(any(), any(), any(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(ackResult));

        // Build a valid receipt handle string
        String receiptHandle = buildReceiptHandle(TEST_TOPIC, System.currentTimeMillis(), 30000L);

        JSONObject result = activity.ackMessage(proxyContext, TEST_TOPIC, TEST_GROUP, receiptHandle).get();

        assertThat(result.getString("status")).isEqualTo("OK");
    }

    @Test
    public void testAckMessage_emptyReceiptHandle_throwsIllegalArgument() {
        CompletableFuture<JSONObject> future = activity.ackMessage(proxyContext, TEST_TOPIC, TEST_GROUP, "");

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("receiptHandle cannot be empty");
    }

    @Test
    public void testAckMessage_emptyConsumerGroup_throwsIllegalArgument() {
        String receiptHandle = buildReceiptHandle(TEST_TOPIC, System.currentTimeMillis(), 30000L);

        CompletableFuture<JSONObject> future = activity.ackMessage(proxyContext, TEST_TOPIC, "", receiptHandle);

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("consumerGroup cannot be empty");
    }

    // ─── changeInvisibleTime ────────────────────────────────────────────────────

    @Test
    public void testChangeInvisibleTime_success() throws Exception {
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);

        when(messagingProcessor.changeInvisibleTime(any(), any(), any(), anyString(), anyString(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(ackResult));

        String receiptHandle = buildReceiptHandle(TEST_TOPIC, System.currentTimeMillis(), 30000L);

        JSONObject result = activity.changeInvisibleTime(proxyContext, TEST_TOPIC, TEST_GROUP,
            receiptHandle, 60000L).get();

        assertThat(result.getString("status")).isEqualTo("OK");
    }

    @Test
    public void testChangeInvisibleTime_emptyReceiptHandle_throwsIllegalArgument() {
        CompletableFuture<JSONObject> future = activity.changeInvisibleTime(proxyContext, TEST_TOPIC,
            TEST_GROUP, "", 60000L);

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("receiptHandle cannot be empty");
    }

    @Test
    public void testChangeInvisibleTime_negativeInvisibleTime_throwsIllegalArgument() {
        String receiptHandle = buildReceiptHandle(TEST_TOPIC, System.currentTimeMillis(), 30000L);

        CompletableFuture<JSONObject> future = activity.changeInvisibleTime(proxyContext, TEST_TOPIC,
            TEST_GROUP, receiptHandle, -1L);

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invisibleTime must be positive");
    }

    @Test
    public void testChangeInvisibleTime_emptyConsumerGroup_throwsIllegalArgument() {
        String receiptHandle = buildReceiptHandle(TEST_TOPIC, System.currentTimeMillis(), 30000L);

        CompletableFuture<JSONObject> future = activity.changeInvisibleTime(proxyContext, TEST_TOPIC,
            "", receiptHandle, 60000L);

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
            .hasCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("consumerGroup cannot be empty");
    }

    // ─── helper ─────────────────────────────────────────────────────────────────

    private String buildReceiptHandle(String topic, long popTime, long invisibleTime) {
        return ExtraInfoUtil.buildExtraInfo(
            12345,
            popTime,
            invisibleTime,
            0,
            topic,
            "brokerName",
            0,
            100
        );
    }
}
