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
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HttpRequestPipelineTest {

    private ProxyContext context;
    private FullHttpRequest request;

    @Before
    public void setUp() {
        context = ProxyContext.create();
        request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
            "/queues/test-topic/messages", Unpooled.EMPTY_BUFFER);
    }

    @Test
    public void testSinglePipelineExecutes() {
        List<String> executionOrder = new ArrayList<>();
        HttpRequestPipeline singlePipeline = (ctx, req) -> executionOrder.add("step-1");

        singlePipeline.execute(context, request);

        assertThat(executionOrder).containsExactly("step-1");
    }

    @Test
    public void testPipeChainExecutesInOrder() {
        List<String> executionOrder = new ArrayList<>();

        HttpRequestPipeline first = (ctx, req) -> executionOrder.add("first");
        HttpRequestPipeline second = (ctx, req) -> executionOrder.add("second");
        HttpRequestPipeline third = (ctx, req) -> executionOrder.add("third");

        // pipe() appends: first.pipe(second) means second runs after first
        HttpRequestPipeline chain = first.pipe(second).pipe(third);
        chain.execute(context, request);

        assertThat(executionOrder).containsExactly("first", "second", "third");
    }

    @Test
    public void testPipelinePassesContextAndRequest() {
        ProxyContext[] capturedContext = new ProxyContext[1];
        FullHttpRequest[] capturedRequest = new FullHttpRequest[1];

        HttpRequestPipeline capturingPipeline = (ctx, req) -> {
            capturedContext[0] = ctx;
            capturedRequest[0] = req;
        };

        capturingPipeline.execute(context, request);

        assertThat(capturedContext[0]).isSameAs(context);
        assertThat(capturedRequest[0]).isSameAs(request);
    }

    @Test
    public void testPipelineExceptionPropagates() {
        HttpRequestPipeline throwingPipeline = (ctx, req) -> {
            throw new RuntimeException("pipeline error");
        };

        assertThatThrownBy(() -> throwingPipeline.execute(context, request))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("pipeline error");
    }

    @Test
    public void testPipeChainStopsOnException() {
        List<String> executionOrder = new ArrayList<>();

        HttpRequestPipeline first = (ctx, req) -> executionOrder.add("first");
        HttpRequestPipeline throwing = (ctx, req) -> {
            throw new RuntimeException("abort");
        };
        HttpRequestPipeline third = (ctx, req) -> executionOrder.add("third");

        HttpRequestPipeline chain = first.pipe(throwing).pipe(third);

        assertThatThrownBy(() -> chain.execute(context, request))
            .isInstanceOf(RuntimeException.class);

        // "third" should not have been reached
        assertThat(executionOrder).containsExactly("first");
    }

    @Test
    public void testContextInitPipelineIntegratesWithChain() {
        HttpContextInitPipeline contextInitPipeline = new HttpContextInitPipeline();
        List<String> executionOrder = new ArrayList<>();

        HttpRequestPipeline preStep = (ctx, req) -> executionOrder.add("pre");
        HttpRequestPipeline postStep = (ctx, req) -> executionOrder.add("post");

        request.headers().set("x-mq-request-id", "chain-req-id");

        HttpRequestPipeline chain = preStep.pipe(contextInitPipeline).pipe(postStep);
        chain.execute(context, request);

        assertThat(executionOrder).containsExactly("pre", "post");
        assertThat(context.getRequestId()).isEqualTo("chain-req-id");
    }
}
