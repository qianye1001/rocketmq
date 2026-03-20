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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HttpErrorResponseBuilderTest {

    private JSONObject parseResponseBody(FullHttpResponse response) {
        byte[] bytes = new byte[response.content().readableBytes()];
        response.content().readBytes(bytes);
        return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    public void testBuildJsonOkResponse_returnsStatus200() {
        JSONObject payload = new JSONObject();
        payload.put("messageId", "msg-001");

        FullHttpResponse response = HttpErrorResponseBuilder.buildJsonOkResponse(payload, "req-001");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("messageId")).isEqualTo("msg-001");
        assertThat(body.getString("requestId")).isEqualTo("req-001");
    }

    @Test
    public void testBuildJsonOkResponse_withNullRequestId() {
        JSONObject payload = new JSONObject();
        payload.put("key", "value");

        FullHttpResponse response = HttpErrorResponseBuilder.buildJsonOkResponse(payload, null);

        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("key")).isEqualTo("value");
        assertThat(body.containsKey("requestId")).isFalse();
    }

    @Test
    public void testBuildErrorResponse_forAuthenticationException_returns401() {
        AuthenticationException exception = new AuthenticationException("invalid credentials");

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-auth");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.UNAUTHORIZED);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("AuthenticationFailed");
        assertThat(body.getString("requestId")).isEqualTo("req-auth");
    }

    @Test
    public void testBuildErrorResponse_forAuthorizationException_returns403() {
        AuthorizationException exception = new AuthorizationException("access denied");

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-authz");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.FORBIDDEN);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("AuthorizationFailed");
    }

    @Test
    public void testBuildErrorResponse_forAclException_returns403() {
        AclException exception = new AclException("acl denied");

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-acl");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.FORBIDDEN);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("AccessDenied");
    }

    @Test
    public void testBuildErrorResponse_forMQClientException_returns400() {
        MQClientException exception = new MQClientException("bad request param", null);

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-client");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("ClientError");
    }

    @Test
    public void testBuildErrorResponse_forMQBrokerException_returns500() {
        MQBrokerException exception = new MQBrokerException(500, "broker internal error");

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-broker");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("BrokerError");
    }

    @Test
    public void testBuildErrorResponse_forIllegalArgumentException_returns400() {
        IllegalArgumentException exception = new IllegalArgumentException("topic cannot be empty");

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-arg");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("InvalidParameter");
        assertThat(body.getString("message")).isEqualTo("topic cannot be empty");
    }

    @Test
    public void testBuildErrorResponse_forProxyException_withForbiddenCode_returns403() {
        ProxyException exception = new ProxyException(ProxyExceptionCode.FORBIDDEN, "forbidden resource");

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-proxy");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.FORBIDDEN);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("FORBIDDEN");
    }

    @Test
    public void testBuildErrorResponse_forUnknownException_returns500() {
        RuntimeException exception = new RuntimeException("unexpected error");

        FullHttpResponse response = HttpErrorResponseBuilder.buildErrorResponse(exception, "req-unknown");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("InternalError");
        assertThat(body.getString("message")).isEqualTo("unexpected error");
    }

    @Test
    public void testBuildNotFoundResponse() {
        FullHttpResponse response = HttpErrorResponseBuilder.buildNotFoundResponse("req-404");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.NOT_FOUND);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("NotFound");
    }

    @Test
    public void testBuildMethodNotAllowedResponse() {
        FullHttpResponse response = HttpErrorResponseBuilder.buildMethodNotAllowedResponse("req-405");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("MethodNotAllowed");
    }

    @Test
    public void testBuildTooManyRequestsResponse() {
        FullHttpResponse response = HttpErrorResponseBuilder.buildTooManyRequestsResponse("req-429");

        assertThat(response.status()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
        JSONObject body = parseResponseBody(response);
        assertThat(body.getString("code")).isEqualTo("Throttled");
    }

    @Test
    public void testBuildJsonErrorResponse_setsContentTypeHeader() {
        FullHttpResponse response = HttpErrorResponseBuilder.buildJsonErrorResponse(
            HttpResponseStatus.BAD_REQUEST, "InvalidParameter", "bad param", "req-ct");

        assertThat(response.headers().get("Content-Type")).contains("application/json");
    }

    @Test
    public void testBuildJsonErrorResponse_setsContentLengthHeader() {
        FullHttpResponse response = HttpErrorResponseBuilder.buildJsonErrorResponse(
            HttpResponseStatus.BAD_REQUEST, "InvalidParameter", "bad param", "req-cl");

        String contentLength = response.headers().get("Content-Length");
        assertThat(contentLength).isNotNull();
        assertThat(Integer.parseInt(contentLength)).isGreaterThan(0);
    }
}
