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

import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class HttpAuthenticationPipeline implements HttpRequestPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_ACCESS_KEY = "x-mq-access-key";
    private static final String HEADER_SIGNATURE = "x-mq-signature";

    private final AuthConfig authConfig;
    private final AuthenticationEvaluator authenticationEvaluator;

    public HttpAuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
        this.authConfig = authConfig;
        this.authenticationEvaluator = AuthenticationFactory.getEvaluator(authConfig, messagingProcessor::getMetadataService);
    }

    @Override
    public void execute(ProxyContext context, FullHttpRequest request) {
        if (!authConfig.isAuthenticationEnabled()) {
            return;
        }
        try {
            String accessKey = request.headers().get(HEADER_ACCESS_KEY);
            String signature = request.headers().get(HEADER_SIGNATURE);

            if (accessKey != null && signature != null) {
                DefaultAuthenticationContext authenticationContext = new DefaultAuthenticationContext();
                authenticationContext.setUsername(accessKey);
                authenticationContext.setSignature(signature);
                authenticationEvaluator.evaluate(authenticationContext);
            }
        } catch (AuthenticationException ex) {
            throw ex;
        } catch (Throwable ex) {
            LOGGER.error("HTTP authenticate failed, uri:{}", request.uri(), ex);
            throw ex;
        }
    }
}
