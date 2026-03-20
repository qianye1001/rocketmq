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
package org.apache.rocketmq.proxy.http.pipeline;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HttpContextInitPipelineTest {

    private HttpContextInitPipeline pipeline;
    private ProxyContext context;

    @Before
    public void setUp() {
        pipeline = new HttpContextInitPipeline();
        context = ProxyContext.create();
    }

    private FullHttpRequest buildRequest(String uri) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri, Unpooled.EMPTY_BUFFER);
    }

    @Test
    public void testExecute_setsProtocolTypeToHttp() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");
        pipeline.execute(context, request);

        assertThat(context.getProtocolType()).isEqualTo(ChannelProtocolType.HTTP.getName());
    }

    @Test
    public void testExecute_setsClientIdFromHeader() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");
        request.headers().set("x-mq-client-id", "my-client-001");

        pipeline.execute(context, request);

        assertThat(context.getClientID()).isEqualTo("my-client-001");
    }

    @Test
    public void testExecute_setsLanguageFromHeader() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");
        request.headers().set("x-mq-language", "JAVA");

        pipeline.execute(context, request);

        assertThat(context.getLanguage()).isEqualTo("JAVA");
    }

    @Test
    public void testExecute_defaultsLanguageToHttpWhenHeaderAbsent() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");

        pipeline.execute(context, request);

        assertThat(context.getLanguage()).isEqualTo("HTTP");
    }

    @Test
    public void testExecute_setsClientVersionFromHeader() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");
        request.headers().set("x-mq-client-version", "5.0.0");

        pipeline.execute(context, request);

        assertThat(context.getClientVersion()).isEqualTo("5.0.0");
    }

    @Test
    public void testExecute_setsRequestIdFromHeader() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");
        request.headers().set("x-mq-request-id", "req-12345");

        pipeline.execute(context, request);

        assertThat(context.getRequestId()).isEqualTo("req-12345");
    }

    @Test
    public void testExecute_generatesRequestIdWhenHeaderAbsent() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");

        pipeline.execute(context, request);

        // A UUID should be auto-generated
        assertThat(context.getRequestId()).isNotBlank();
        assertThat(context.getRequestId()).matches(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    public void testExecute_setsNamespaceFromHeader() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");
        request.headers().set("x-mq-namespace", "my-namespace");

        pipeline.execute(context, request);

        assertThat(context.getNamespace()).isEqualTo("my-namespace");
    }

    @Test
    public void testExecute_setsAllHeadersAtOnce() {
        FullHttpRequest request = buildRequest("/queues/test-topic/messages");
        request.headers()
            .set("x-mq-client-id", "client-abc")
            .set("x-mq-language", "GO")
            .set("x-mq-client-version", "4.9.0")
            .set("x-mq-request-id", "req-xyz")
            .set("x-mq-namespace", "ns-prod");

        pipeline.execute(context, request);

        assertThat(context.getClientID()).isEqualTo("client-abc");
        assertThat(context.getLanguage()).isEqualTo("GO");
        assertThat(context.getClientVersion()).isEqualTo("4.9.0");
        assertThat(context.getRequestId()).isEqualTo("req-xyz");
        assertThat(context.getNamespace()).isEqualTo("ns-prod");
        assertThat(context.getProtocolType()).isEqualTo(ChannelProtocolType.HTTP.getName());
    }
}
