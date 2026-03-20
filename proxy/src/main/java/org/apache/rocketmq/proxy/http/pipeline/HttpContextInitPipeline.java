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
import io.netty.handler.codec.http.HttpHeaders;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;

public class HttpContextInitPipeline implements HttpRequestPipeline {

    private static final String HEADER_CLIENT_ID = "x-mq-client-id";
    private static final String HEADER_LANGUAGE = "x-mq-language";
    private static final String HEADER_CLIENT_VERSION = "x-mq-client-version";
    private static final String HEADER_REQUEST_ID = "x-mq-request-id";
    private static final String HEADER_NAMESPACE = "x-mq-namespace";

    @Override
    public void execute(ProxyContext context, FullHttpRequest request) {
        HttpHeaders headers = request.headers();
        String requestId = getHeaderOrDefault(headers, HEADER_REQUEST_ID, "");
        if (StringUtils.isBlank(requestId)) {
            requestId = UUID.randomUUID().toString();
        }
        context.setProtocolType(ChannelProtocolType.HTTP.getName())
            .setClientID(getHeaderOrDefault(headers, HEADER_CLIENT_ID, ""))
            .setLanguage(getHeaderOrDefault(headers, HEADER_LANGUAGE, "HTTP"))
            .setClientVersion(getHeaderOrDefault(headers, HEADER_CLIENT_VERSION, ""))
            .setRequestId(requestId)
            .setNamespace(getHeaderOrDefault(headers, HEADER_NAMESPACE, ""));
    }

    private String getHeaderOrDefault(HttpHeaders headers, String name, String defaultValue) {
        String value = headers.get(name);
        return value != null ? value : defaultValue;
    }
}
