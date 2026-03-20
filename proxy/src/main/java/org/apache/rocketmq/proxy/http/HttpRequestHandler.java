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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.http.common.HttpErrorResponseBuilder;
import org.apache.rocketmq.proxy.http.pipeline.HttpRequestPipeline;

/**
 * Netty ChannelHandler that routes incoming HTTP requests to the HttpMessagingActivity.
 * <p>
 * API routes:
 * <pre>
 *   POST   /queues/{topic}/messages                    -> sendMessage
 *   GET    /queues/{topic}/messages?consumerGroup=&...  -> receiveMessage
 *   DELETE /queues/{topic}/messages?receiptHandle=&...  -> ackMessage
 *   PUT    /queues/{topic}/messages?receiptHandle=&...  -> changeInvisibleTime
 * </pre>
 */
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final String PATH_PREFIX = "/queues/";
    private static final String PATH_MESSAGES_SUFFIX = "/messages";

    private final HttpMessagingActivity messagingActivity;
    private final HttpRequestPipeline requestPipeline;
    private final ThreadPoolExecutor executor;

    public HttpRequestHandler(HttpMessagingActivity messagingActivity, HttpRequestPipeline requestPipeline,
        ThreadPoolExecutor executor) {
        this.messagingActivity = messagingActivity;
        this.requestPipeline = requestPipeline;
        this.executor = executor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        ProxyContext proxyContext = ProxyContext.create();

        // set remote/local address
        if (ctx.channel().remoteAddress() instanceof InetSocketAddress) {
            InetSocketAddress remoteAddr = (InetSocketAddress) ctx.channel().remoteAddress();
            proxyContext.setRemoteAddress(remoteAddr.getAddress().getHostAddress() + ":" + remoteAddr.getPort());
        }
        if (ctx.channel().localAddress() instanceof InetSocketAddress) {
            InetSocketAddress localAddr = (InetSocketAddress) ctx.channel().localAddress();
            proxyContext.setLocalAddress(localAddr.getAddress().getHostAddress() + ":" + localAddr.getPort());
        }
        proxyContext.setChannel(ctx.channel());

        // run pipeline (context init, auth)
        try {
            requestPipeline.execute(proxyContext, request);
        } catch (Throwable t) {
            writeResponse(ctx, HttpErrorResponseBuilder.buildErrorResponse(t, proxyContext.getRequestId()));
            return;
        }

        String requestId = proxyContext.getRequestId();

        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String path = decoder.path();
        Map<String, List<String>> params = decoder.parameters();

        // route: /queues/{topic}/messages
        if (!path.startsWith(PATH_PREFIX)) {
            writeResponse(ctx, HttpErrorResponseBuilder.buildNotFoundResponse(requestId));
            return;
        }

        String remaining = path.substring(PATH_PREFIX.length());
        if (!remaining.endsWith(PATH_MESSAGES_SUFFIX)) {
            writeResponse(ctx, HttpErrorResponseBuilder.buildNotFoundResponse(requestId));
            return;
        }

        String topic = remaining.substring(0, remaining.length() - PATH_MESSAGES_SUFFIX.length());
        if (topic.isEmpty()) {
            writeResponse(ctx, HttpErrorResponseBuilder.buildJsonErrorResponse(
                HttpResponseStatus.BAD_REQUEST, "InvalidParameter", "topic cannot be empty", requestId));
            return;
        }

        proxyContext.setAction("Http" + request.method().name());

        // retain the request so we can read body in the executor thread
        request.retain();
        try {
            executor.submit(() -> {
                try {
                    dispatch(ctx, proxyContext, request, topic, params, requestId);
                } finally {
                    request.release();
                }
            });
        } catch (Throwable t) {
            request.release();
            writeResponse(ctx, HttpErrorResponseBuilder.buildTooManyRequestsResponse(requestId));
        }
    }

    private void dispatch(ChannelHandlerContext ctx, ProxyContext proxyContext, FullHttpRequest request,
        String topic, Map<String, List<String>> params, String requestId) {
        HttpMethod method = request.method();
        CompletableFuture<JSONObject> future;

        try {
            if (HttpMethod.POST.equals(method)) {
                // SendMessage
                String bodyStr = request.content().toString(StandardCharsets.UTF_8);
                JSONObject body = JSON.parseObject(bodyStr);
                if (body == null) {
                    body = new JSONObject();
                }
                future = messagingActivity.sendMessage(proxyContext, topic, body);

            } else if (HttpMethod.GET.equals(method)) {
                // ReceiveMessage
                String consumerGroup = getParam(params, "consumerGroup");
                int maxMsgNums = getIntParam(params, "maxMsgNums", 1);
                long invisibleTime = getLongParam(params, "invisibleTime", 0);
                long waitTimeMillis = getLongParam(params, "waitTimeMillis", 0);
                future = messagingActivity.receiveMessage(proxyContext, topic, consumerGroup,
                    maxMsgNums, invisibleTime, waitTimeMillis);

            } else if (HttpMethod.DELETE.equals(method)) {
                // AckMessage
                String consumerGroup = getParam(params, "consumerGroup");
                String receiptHandle = getParam(params, "receiptHandle");
                future = messagingActivity.ackMessage(proxyContext, topic, consumerGroup, receiptHandle);

            } else if (HttpMethod.PUT.equals(method)) {
                // ChangeInvisibleTime
                String consumerGroup = getParam(params, "consumerGroup");
                String receiptHandle = getParam(params, "receiptHandle");
                long invisibleTime = getLongParam(params, "invisibleTime", 0);
                future = messagingActivity.changeInvisibleTime(proxyContext, topic, consumerGroup,
                    receiptHandle, invisibleTime);

            } else {
                writeResponse(ctx, HttpErrorResponseBuilder.buildMethodNotAllowedResponse(requestId));
                return;
            }

            future.thenAccept(result -> {
                FullHttpResponse response = HttpErrorResponseBuilder.buildJsonOkResponse(result, requestId);
                writeResponse(ctx, response);
            }).exceptionally(t -> {
                writeResponse(ctx, HttpErrorResponseBuilder.buildErrorResponse(t, requestId));
                return null;
            });
        } catch (Throwable t) {
            writeResponse(ctx, HttpErrorResponseBuilder.buildErrorResponse(t, requestId));
        }
    }

    private void writeResponse(ChannelHandlerContext ctx, FullHttpResponse response) {
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    private String getParam(Map<String, List<String>> params, String name) {
        List<String> values = params.get(name);
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        return null;
    }

    private int getIntParam(Map<String, List<String>> params, String name, int defaultValue) {
        String value = getParam(params, name);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private long getLongParam(Map<String, List<String>> params, String name, long defaultValue) {
        String value = getParam(params, name);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("HTTP request handler exception", cause);
        if (ctx.channel().isActive()) {
            writeResponse(ctx, HttpErrorResponseBuilder.buildErrorResponse(cause, null));
        }
    }
}
