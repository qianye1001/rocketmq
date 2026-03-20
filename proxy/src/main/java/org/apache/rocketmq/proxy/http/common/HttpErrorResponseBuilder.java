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
package org.apache.rocketmq.proxy.http.common;

import com.alibaba.fastjson2.JSONObject;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

public class HttpErrorResponseBuilder {

    public static FullHttpResponse buildErrorResponse(Throwable t, String requestId) {
        t = ExceptionUtils.getRealException(t);
        HttpResponseStatus status;
        String errorCode;
        String errorMessage;

        if (t instanceof ProxyException) {
            ProxyException e = (ProxyException) t;
            errorCode = e.getCode().name();
            errorMessage = e.getMessage();
            if (e.getCode() == ProxyExceptionCode.FORBIDDEN) {
                status = HttpResponseStatus.FORBIDDEN;
            } else {
                status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            }
        } else if (t instanceof AuthenticationException) {
            status = HttpResponseStatus.UNAUTHORIZED;
            errorCode = "AuthenticationFailed";
            errorMessage = t.getMessage();
        } else if (t instanceof AuthorizationException) {
            status = HttpResponseStatus.FORBIDDEN;
            errorCode = "AuthorizationFailed";
            errorMessage = t.getMessage();
        } else if (t instanceof AclException) {
            status = HttpResponseStatus.FORBIDDEN;
            errorCode = "AccessDenied";
            errorMessage = t.getMessage();
        } else if (t instanceof MQClientException) {
            MQClientException e = (MQClientException) t;
            status = HttpResponseStatus.BAD_REQUEST;
            errorCode = "ClientError";
            errorMessage = e.getErrorMessage();
        } else if (t instanceof MQBrokerException) {
            MQBrokerException e = (MQBrokerException) t;
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            errorCode = "BrokerError";
            errorMessage = e.getErrorMessage();
        } else if (t instanceof IllegalArgumentException) {
            status = HttpResponseStatus.BAD_REQUEST;
            errorCode = "InvalidParameter";
            errorMessage = t.getMessage();
        } else {
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            errorCode = "InternalError";
            errorMessage = t.getMessage() != null ? t.getMessage() : "Unknown error";
        }

        return buildJsonErrorResponse(status, errorCode, errorMessage, requestId);
    }

    public static FullHttpResponse buildJsonErrorResponse(HttpResponseStatus status, String errorCode,
        String errorMessage, String requestId) {
        JSONObject json = new JSONObject();
        json.put("code", errorCode);
        json.put("message", errorMessage);
        if (requestId != null) {
            json.put("requestId", requestId);
        }
        byte[] body = json.toJSONString().getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
            Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length);
        return response;
    }

    public static FullHttpResponse buildJsonOkResponse(JSONObject json, String requestId) {
        if (requestId != null) {
            json.put("requestId", requestId);
        }
        byte[] body = json.toJSONString().getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
            Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length);
        return response;
    }

    public static FullHttpResponse buildNotFoundResponse(String requestId) {
        return buildJsonErrorResponse(HttpResponseStatus.NOT_FOUND, "NotFound",
            "The requested resource was not found", requestId);
    }

    public static FullHttpResponse buildMethodNotAllowedResponse(String requestId) {
        return buildJsonErrorResponse(HttpResponseStatus.METHOD_NOT_ALLOWED, "MethodNotAllowed",
            "The HTTP method is not allowed for this resource", requestId);
    }

    public static FullHttpResponse buildTooManyRequestsResponse(String requestId) {
        return buildJsonErrorResponse(HttpResponseStatus.TOO_MANY_REQUESTS, "Throttled",
            "Too many requests, please retry later", requestId);
    }
}
