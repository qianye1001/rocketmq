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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.http.pipeline.HttpAuthenticationPipeline;
import org.apache.rocketmq.proxy.http.pipeline.HttpAuthorizationPipeline;
import org.apache.rocketmq.proxy.http.pipeline.HttpContextInitPipeline;
import org.apache.rocketmq.proxy.http.pipeline.HttpRequestPipeline;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class HttpServer implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final int port;
    private final HttpMessagingActivity messagingActivity;
    private final HttpRequestPipeline requestPipeline;
    private final ThreadPoolExecutor executor;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public HttpServer(MessagingProcessor messagingProcessor) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.port = config.getHttpServerPort();

        // build request pipeline
        HttpRequestPipeline pipeline = (ctx, request) -> {
        };
        AuthConfig authConfig = ConfigurationManager.getAuthConfig();
        if (authConfig != null) {
            pipeline = pipeline
                .pipe(new HttpAuthorizationPipeline(authConfig, messagingProcessor))
                .pipe(new HttpAuthenticationPipeline(authConfig, messagingProcessor));
        }
        pipeline = pipeline.pipe(new HttpContextInitPipeline());
        this.requestPipeline = pipeline;

        // build activity
        this.messagingActivity = new DefaultHttpMessagingActivity(messagingProcessor);

        // build thread pool
        this.executor = ThreadPoolMonitor.createAndMonitor(
            config.getHttpThreadPoolNums(),
            config.getHttpThreadPoolNums(),
            1, TimeUnit.MINUTES,
            "HttpRequestExecutorThread",
            config.getHttpThreadPoolQueueCapacity()
        );
    }

    @Override
    public void start() throws Exception {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new IdleStateHandler(0, 0, config.getHttpIdleTimeoutSeconds()))
                        .addLast(new HttpServerCodec())
                        .addLast(new HttpObjectAggregator(config.getHttpMaxContentLength()))
                        .addLast(new HttpRequestHandler(messagingActivity, requestPipeline, executor));
                }
            })
            .option(ChannelOption.SO_BACKLOG, 1024)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        this.serverChannel = bootstrap.bind(port).sync().channel();
        log.info("HTTP server started on port {}", port);
    }

    @Override
    public void shutdown() throws Exception {
        if (serverChannel != null) {
            serverChannel.close().sync();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (executor != null) {
            executor.shutdown();
        }
        log.info("HTTP server shutdown successfully");
    }
}
