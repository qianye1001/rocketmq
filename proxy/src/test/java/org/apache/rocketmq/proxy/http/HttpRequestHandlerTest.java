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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.http.pipeline.HttpRequestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class HttpRequestHandlerTest {

    @Mock
    private HttpMessagingActivity messagingActivity;

    private HttpRequestPipeline noOpPipeline;
    private ThreadPoolExecutor executor;
    private EmbeddedChannel embeddedChannel;

    @Before
    public void setUp() {
        noOpPipeline = (ctx, req) -> {
            // no-op: sets requestId via context init
            ctx.setRequestId("test-request-id");
        };
        executor = new ThreadPoolExecutor(
            2, 2, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));

        HttpRequestHandler handler = new HttpRequestHandler(messagingActivity, noOpPipeline, executor);
        embeddedChannel = new EmbeddedChannel(handler);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
        embeddedChannel.close();
    }

    private FullHttpRequest buildRequest(HttpMethod method, String uri, String body) {
        ByteBuf content = body != null
            ? Unpooled.copiedBuffer(body, StandardCharsets.UTF_8)
            : Unpooled.EMPTY_BUFFER;
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri, content);
    }

    private JSONObject readResponseBody(FullHttpResponse response) {
        byte[] bytes = new byte[response.content().readableBytes()];
        response.content().readBytes(bytes);
        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    public void testSendMessage_routesToMessagingActivity() throws Exception {
        JSONObject sendResult = new JSONObject();
        sendResult.put("messageId", "msg-001");
        sendResult.put("sendStatus", "SEND_OK");
        when(messagingActivity.sendMessage(any(ProxyContext.class), anyString(), any(JSONObject.class)))
            .thenReturn(CompletableFuture.completedFuture(sendResult));

        String requestBody = "{\"body\":\"hello world\",\"tags\":\"TagA\"}";
        FullHttpRequest request = buildRequest(HttpMethod.POST, "/queues/TestTopic/messages", requestBody);
        embeddedChannel.writeInbound(request);

        // Wait for async executor to complete
        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);

        JSONObject body = readResponseBody(response);
        assertThat(body.getString("messageId")).isEqualTo("msg-001");
    }

    @Test
    public void testReceiveMessage_routesToMessagingActivity() throws Exception {
        JSONObject receiveResult = new JSONObject();
        receiveResult.put("popStatus", "FOUND");
        when(messagingActivity.receiveMessage(any(ProxyContext.class), anyString(), anyString(),
            anyInt(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(receiveResult));

        FullHttpRequest request = buildRequest(HttpMethod.GET,
            "/queues/TestTopic/messages?consumerGroup=TestGroup&maxMsgNums=5", null);
        embeddedChannel.writeInbound(request);

        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    public void testAckMessage_routesToMessagingActivity() throws Exception {
        JSONObject ackResult = new JSONObject();
        ackResult.put("status", "OK");
        when(messagingActivity.ackMessage(any(ProxyContext.class), anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(ackResult));

        FullHttpRequest request = buildRequest(HttpMethod.DELETE,
            "/queues/TestTopic/messages?consumerGroup=TestGroup&receiptHandle=handle-abc", null);
        embeddedChannel.writeInbound(request);

        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    public void testChangeInvisibleTime_routesToMessagingActivity() throws Exception {
        JSONObject changeResult = new JSONObject();
        changeResult.put("status", "OK");
        when(messagingActivity.changeInvisibleTime(any(ProxyContext.class), anyString(), anyString(),
            anyString(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(changeResult));

        FullHttpRequest request = buildRequest(HttpMethod.PUT,
            "/queues/TestTopic/messages?consumerGroup=TestGroup&receiptHandle=handle-abc&invisibleTime=30000",
            null);
        embeddedChannel.writeInbound(request);

        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    public void testInvalidPath_returnsNotFound() throws Exception {
        FullHttpRequest request = buildRequest(HttpMethod.GET, "/invalid/path", null);
        embeddedChannel.writeInbound(request);

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.NOT_FOUND);

        JSONObject body = readResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("NotFound");
    }

    @Test
    public void testPathWithoutMessagesSuffix_returnsNotFound() throws Exception {
        FullHttpRequest request = buildRequest(HttpMethod.GET, "/queues/TestTopic/other", null);
        embeddedChannel.writeInbound(request);

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.NOT_FOUND);
    }

    @Test
    public void testEmptyTopic_returnsBadRequest() throws Exception {
        // Path: /queues//messages — topic is empty
        FullHttpRequest request = buildRequest(HttpMethod.POST, "/queues//messages",
            "{\"body\":\"hello\"}");
        embeddedChannel.writeInbound(request);

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);

        JSONObject body = readResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("InvalidParameter");
    }

    @Test
    public void testUnsupportedHttpMethod_returnsMethodNotAllowed() throws Exception {
        FullHttpRequest request = buildRequest(HttpMethod.PATCH, "/queues/TestTopic/messages", null);
        embeddedChannel.writeInbound(request);

        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED);

        JSONObject body = readResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("MethodNotAllowed");
    }

    @Test
    public void testPipelineException_returnsErrorResponse() throws Exception {
        HttpRequestPipeline throwingPipeline = (ctx, req) -> {
            throw new RuntimeException("pipeline failure");
        };
        HttpRequestHandler handler = new HttpRequestHandler(messagingActivity, throwingPipeline, executor);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        FullHttpRequest request = buildRequest(HttpMethod.POST, "/queues/TestTopic/messages",
            "{\"body\":\"hello\"}");
        channel.writeInbound(request);

        FullHttpResponse response = channel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR);

        channel.close();
    }

    @Test
    public void testSendMessage_activityFails_returnsErrorResponse() throws Exception {
        CompletableFuture<JSONObject> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalArgumentException("message body cannot be empty"));
        when(messagingActivity.sendMessage(any(ProxyContext.class), anyString(), any(JSONObject.class)))
            .thenReturn(failedFuture);

        FullHttpRequest request = buildRequest(HttpMethod.POST, "/queues/TestTopic/messages",
            "{\"body\":\"\"}");
        embeddedChannel.writeInbound(request);

        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        FullHttpResponse response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    public void testSendMessage_passesTopicCorrectly() throws Exception {
        JSONObject sendResult = new JSONObject();
        sendResult.put("messageId", "msg-002");
        sendResult.put("sendStatus", "SEND_OK");

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        when(messagingActivity.sendMessage(any(ProxyContext.class), topicCaptor.capture(), any(JSONObject.class)))
            .thenReturn(CompletableFuture.completedFuture(sendResult));

        FullHttpRequest request = buildRequest(HttpMethod.POST, "/queues/MySpecialTopic/messages",
            "{\"body\":\"hello\"}");
        embeddedChannel.writeInbound(request);

        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        assertThat(topicCaptor.getValue()).isEqualTo("MySpecialTopic");
    }

    @Test
    public void testReceiveMessage_passesQueryParamsCorrectly() throws Exception {
        JSONObject receiveResult = new JSONObject();
        receiveResult.put("popStatus", "NO_NEW_MSG");

        ArgumentCaptor<String> groupCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> maxMsgCaptor = ArgumentCaptor.forClass(Integer.class);
        when(messagingActivity.receiveMessage(any(ProxyContext.class), anyString(),
            groupCaptor.capture(), maxMsgCaptor.capture(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(receiveResult));

        FullHttpRequest request = buildRequest(HttpMethod.GET,
            "/queues/TestTopic/messages?consumerGroup=MyGroup&maxMsgNums=10", null);
        embeddedChannel.writeInbound(request);

        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        embeddedChannel.runPendingTasks();

        assertThat(groupCaptor.getValue()).isEqualTo("MyGroup");
        assertThat(maxMsgCaptor.getValue()).isEqualTo(10);
    }
}
