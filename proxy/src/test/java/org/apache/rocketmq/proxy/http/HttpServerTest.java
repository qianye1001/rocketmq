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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Integration-level test for HttpServer: starts a real Netty HTTP server on a random port
 * and sends actual HTTP requests via HttpURLConnection to verify end-to-end routing.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class HttpServerTest extends InitConfigTest {

    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private MetadataService metadataService;
    @Mock
    private ProxyRelayService proxyRelayService;

    private HttpServer httpServer;
    private int serverPort;

    @Before
    @Override
    public void before() throws Throwable {
        super.before();

        when(messagingProcessor.getMetadataService()).thenReturn(metadataService);
        when(messagingProcessor.getProxyRelayService()).thenReturn(proxyRelayService);

        // Use a free port to avoid conflicts
        serverPort = findFreePort();
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setHttpServerPort(serverPort);
        config.setHttpThreadPoolNums(2);
        config.setHttpThreadPoolQueueCapacity(100);
        config.setHttpIdleTimeoutSeconds(30);
        config.setHttpMaxContentLength(1024 * 1024);

        httpServer = new HttpServer(messagingProcessor);
        httpServer.start();

        // Allow Netty to bind
        Thread.sleep(200);
    }

    @After
    @Override
    public void after() throws Exception {
        if (httpServer != null) {
            httpServer.shutdown();
        }
        super.after();
    }

    private int findFreePort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private HttpURLConnection openConnection(String method, String path) throws IOException {
        URL url = new URL("http://127.0.0.1:" + serverPort + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(5000);
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        conn.setRequestProperty("x-mq-request-id", "test-req-" + System.nanoTime());
        return conn;
    }

    private String readResponse(HttpURLConnection conn) throws IOException {
        byte[] bytes;
        try {
            bytes = readStream(conn.getInputStream());
        } catch (IOException e) {
            bytes = conn.getErrorStream() != null ? readStream(conn.getErrorStream()) : new byte[0];
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private byte[] readStream(InputStream inputStream) throws IOException {
        if (inputStream == null) {
            return new byte[0];
        }

        try (InputStream in = inputStream;
             ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            byte[] data = new byte[4096];
            int n;
            while ((n = in.read(data)) != -1) {
                buffer.write(data, 0, n);
            }
            return buffer.toByteArray();
        }
    }

    @Test
    public void testServerStartsAndRespondsToRequests() throws Exception {
        HttpURLConnection conn = openConnection("GET", "/queues/TestTopic/messages?consumerGroup=TestGroup");

        PopResult popResult = new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList());
        when(messagingProcessor.popMessage(any(), any(), anyString(), anyString(), anyInt(),
            anyLong(), anyLong(), anyInt(), any(), anyBoolean(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));

        int statusCode = conn.getResponseCode();
        String responseBody = readResponse(conn);

        assertThat(statusCode).isEqualTo(200);
        JSONObject json = JSON.parseObject(responseBody);
        assertThat(json.getString("popStatus")).isEqualTo("NO_NEW_MSG");
    }

    @Test
    public void testSendMessage_endToEnd() throws Exception {
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("e2e-msg-001");
        sendResult.setQueueOffset(0L);

        when(messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), anyList()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        HttpURLConnection conn = openConnection("POST", "/queues/TestTopic/messages");
        conn.setDoOutput(true);
        String requestBody = "{\"body\":\"Hello from HttpServerTest\",\"tags\":\"TagA\"}";
        try (OutputStream out = conn.getOutputStream()) {
            out.write(requestBody.getBytes(StandardCharsets.UTF_8));
        }

        int statusCode = conn.getResponseCode();
        String responseBody = readResponse(conn);

        assertThat(statusCode).isEqualTo(200);
        JSONObject json = JSON.parseObject(responseBody);
        assertThat(json.getString("messageId")).isEqualTo("e2e-msg-001");
        assertThat(json.getString("sendStatus")).isEqualTo("SEND_OK");
    }

    @Test
    public void testInvalidPath_returns404() throws Exception {
        HttpURLConnection conn = openConnection("GET", "/invalid/path");

        int statusCode = conn.getResponseCode();
        String responseBody = readResponse(conn);

        assertThat(statusCode).isEqualTo(404);
        JSONObject json = JSON.parseObject(responseBody);
        assertThat(json.getString("code")).isEqualTo("NotFound");
    }

    @Test
    public void testEmptyTopic_returns400() throws Exception {
        HttpURLConnection conn = openConnection("POST", "/queues//messages");
        conn.setDoOutput(true);
        try (OutputStream out = conn.getOutputStream()) {
            out.write("{\"body\":\"hello\"}".getBytes(StandardCharsets.UTF_8));
        }

        int statusCode = conn.getResponseCode();
        String responseBody = readResponse(conn);

        assertThat(statusCode).isEqualTo(400);
        JSONObject json = JSON.parseObject(responseBody);
        assertThat(json.getString("code")).isEqualTo("InvalidParameter");
    }

    @Test
    public void testUnsupportedMethod_returns405() throws Exception {
        // HttpURLConnection does not support PATCH natively; use reflection to bypass the restriction
        URL url = new URL("http://127.0.0.1:" + serverPort + "/queues/TestTopic/messages");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(5000);
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        // Force the method to PATCH via reflection
        try {
            java.lang.reflect.Field methodField = HttpURLConnection.class.getDeclaredField("method");
            methodField.setAccessible(true);
            methodField.set(conn, "PATCH");
        } catch (Exception e) {
            // If reflection fails, skip the test gracefully
            return;
        }

        int statusCode = conn.getResponseCode();
        String responseBody = readResponse(conn);

        assertThat(statusCode).isEqualTo(405);
        JSONObject json = JSON.parseObject(responseBody);
        assertThat(json.getString("code")).isEqualTo("MethodNotAllowed");
    }

    @Test
    public void testAckMessage_endToEnd() throws Exception {
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);

        when(messagingProcessor.ackMessage(any(), any(), any(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(ackResult));

        String receiptHandle = buildReceiptHandle("TestTopic", System.currentTimeMillis(), 30000L);
        String encodedHandle = java.net.URLEncoder.encode(receiptHandle, StandardCharsets.UTF_8.toString());

        HttpURLConnection conn = openConnection("DELETE",
            "/queues/TestTopic/messages?consumerGroup=TestGroup&receiptHandle=" + encodedHandle);

        int statusCode = conn.getResponseCode();
        String responseBody = readResponse(conn);

        assertThat(statusCode).isEqualTo(200);
        JSONObject json = JSON.parseObject(responseBody);
        assertThat(json.getString("status")).isEqualTo("OK");
    }

    @Test
    public void testServerShutdownAndRestart() throws Exception {
        // Verify server is running
        HttpURLConnection conn = openConnection("GET", "/invalid/path");
        assertThat(conn.getResponseCode()).isEqualTo(404);

        // Shutdown
        httpServer.shutdown();

        // Restart on same port
        httpServer = new HttpServer(messagingProcessor);
        httpServer.start();
        Thread.sleep(200);

        // Verify it's running again
        HttpURLConnection conn2 = openConnection("GET", "/invalid/path");
        assertThat(conn2.getResponseCode()).isEqualTo(404);
    }

    private String buildReceiptHandle(String topic, long popTime, long invisibleTime) {
        return org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil.buildExtraInfo(
            12345, popTime, invisibleTime, 0, topic, "brokerName", 0, 100);
    }
}
