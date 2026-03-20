#!/bin/bash
# =============================================================================
# RocketMQ HTTP Proxy - curl Integration Test Script
# =============================================================================
# Prerequisites:
#   1. NameServer running on localhost:9876
#   2. Broker running in local mode (or cluster mode with NameServer)
#   3. Proxy started with enableHttpServer=true, httpServerPort=8082
#      Example proxy config:
#        proxyMode=local
#        enableHttpServer=true
#        httpServerPort=8082
#
# Usage:
#   chmod +x http-curl-test.sh
#   ./http-curl-test.sh [HOST] [PORT]
#   ./http-curl-test.sh 127.0.0.1 8082
# =============================================================================

HOST="${1:-127.0.0.1}"
PORT="${2:-8082}"
BASE_URL="http://${HOST}:${PORT}"
TOPIC="CurlTestTopic"
CONSUMER_GROUP="CurlTestGroup"
PRODUCER_GROUP="CurlTestProducer"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PASS=0
FAIL=0

print_header() {
    echo ""
    echo -e "${BLUE}══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════════${NC}"
}

assert_status() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"
    local body="$4"

    if [ "$actual" -eq "$expected" ]; then
        echo -e "  ${GREEN}✓ PASS${NC} $test_name (HTTP $actual)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗ FAIL${NC} $test_name (expected HTTP $expected, got HTTP $actual)"
        echo -e "    Response: $body"
        FAIL=$((FAIL + 1))
    fi
}

assert_json_field() {
    local test_name="$1"
    local field="$2"
    local expected="$3"
    local body="$4"

    # Use python3 to parse JSON if available, otherwise grep
    if command -v python3 &>/dev/null; then
        actual=$(echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$field',''))" 2>/dev/null)
    else
        actual=$(echo "$body" | grep -o "\"$field\":\"[^\"]*\"" | sed "s/\"$field\":\"//;s/\"//")
    fi

    if [ "$actual" = "$expected" ]; then
        echo -e "  ${GREEN}✓ PASS${NC} $test_name (.${field} == \"$expected\")"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗ FAIL${NC} $test_name (expected .${field}=\"$expected\", got \"$actual\")"
        FAIL=$((FAIL + 1))
    fi
}

# =============================================================================
# Test 1: Health check — invalid path returns 404
# =============================================================================
print_header "Test 1: Invalid path → 404 Not Found"

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "x-mq-request-id: curl-test-001" \
    "${BASE_URL}/invalid/path")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

assert_status "GET /invalid/path" 404 "$HTTP_CODE" "$BODY"
assert_json_field "error code is NotFound" "code" "NotFound" "$BODY"

# =============================================================================
# Test 2: Send message (POST)
# =============================================================================
print_header "Test 2: Send Message — POST /queues/{topic}/messages"

SEND_BODY=$(cat <<EOF
{
  "body": "Hello from curl test - $(date +%s)",
  "tags": "CurlTag",
  "keys": "curl-key-001",
  "producerGroup": "${PRODUCER_GROUP}",
  "properties": {
    "env": "test",
    "version": "5.0"
  }
}
EOF
)

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "x-mq-request-id: curl-send-001" \
    -H "x-mq-client-id: curl-test-client" \
    -H "x-mq-language: SHELL" \
    -d "$SEND_BODY" \
    "${BASE_URL}/queues/${TOPIC}/messages")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

echo "  Response: $BODY"
assert_status "POST /queues/${TOPIC}/messages" 200 "$HTTP_CODE" "$BODY"
assert_json_field "sendStatus is SEND_OK" "sendStatus" "SEND_OK" "$BODY"

# Extract messageId for later reference
if command -v python3 &>/dev/null; then
    SENT_MESSAGE_ID=$(echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messageId',''))" 2>/dev/null)
    echo -e "  ${YELLOW}→ Sent messageId: ${SENT_MESSAGE_ID}${NC}"
fi

# =============================================================================
# Test 3: Send message with missing body → 400
# =============================================================================
print_header "Test 3: Send Message with empty body → 400 Bad Request"

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "x-mq-request-id: curl-send-002" \
    -d '{"tags":"TagOnly"}' \
    "${BASE_URL}/queues/${TOPIC}/messages")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

assert_status "POST with missing body field" 400 "$HTTP_CODE" "$BODY"
assert_json_field "error code is InvalidParameter" "code" "InvalidParameter" "$BODY"

# =============================================================================
# Test 4: Receive message (GET) — long polling
# =============================================================================
print_header "Test 4: Receive Message — GET /queues/{topic}/messages"

echo "  (Long polling with waitTimeMillis=3000, this may take a few seconds...)"

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X GET \
    -H "x-mq-request-id: curl-recv-001" \
    -H "x-mq-client-id: curl-test-client" \
    "${BASE_URL}/queues/${TOPIC}/messages?consumerGroup=${CONSUMER_GROUP}&maxMsgNums=1&invisibleTime=30000&waitTimeMillis=3000")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

echo "  Response: $BODY"
assert_status "GET /queues/${TOPIC}/messages" 200 "$HTTP_CODE" "$BODY"

# Check popStatus is either FOUND or NO_NEW_MSG
if command -v python3 &>/dev/null; then
    POP_STATUS=$(echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('popStatus',''))" 2>/dev/null)
    echo -e "  ${YELLOW}→ popStatus: ${POP_STATUS}${NC}"

    if [ "$POP_STATUS" = "FOUND" ] || [ "$POP_STATUS" = "NO_NEW_MSG" ]; then
        echo -e "  ${GREEN}✓ PASS${NC} popStatus is valid ($POP_STATUS)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗ FAIL${NC} unexpected popStatus: $POP_STATUS"
        FAIL=$((FAIL + 1))
    fi

    # Extract receiptHandle for ack test
    RECEIPT_HANDLE=$(echo "$BODY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
msgs = d.get('messages', [])
if msgs:
    print(msgs[0].get('receiptHandle', ''))
" 2>/dev/null)

    if [ -n "$RECEIPT_HANDLE" ]; then
        echo -e "  ${YELLOW}→ Got receiptHandle (length: ${#RECEIPT_HANDLE})${NC}"
    fi
fi

# =============================================================================
# Test 5: Receive without consumerGroup → 400
# =============================================================================
print_header "Test 5: Receive without consumerGroup → 400 Bad Request"

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X GET \
    -H "x-mq-request-id: curl-recv-002" \
    "${BASE_URL}/queues/${TOPIC}/messages?maxMsgNums=1")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

assert_status "GET without consumerGroup" 400 "$HTTP_CODE" "$BODY"
assert_json_field "error code is InvalidParameter" "code" "InvalidParameter" "$BODY"

# =============================================================================
# Test 6: Ack message (DELETE) — only if we got a receiptHandle
# =============================================================================
print_header "Test 6: Ack Message — DELETE /queues/{topic}/messages"

if [ -n "$RECEIPT_HANDLE" ]; then
    ENCODED_HANDLE=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$RECEIPT_HANDLE'))" 2>/dev/null || echo "$RECEIPT_HANDLE")

    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X DELETE \
        -H "x-mq-request-id: curl-ack-001" \
        "${BASE_URL}/queues/${TOPIC}/messages?consumerGroup=${CONSUMER_GROUP}&receiptHandle=${ENCODED_HANDLE}")
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    BODY=$(echo "$RESPONSE" | head -1)

    echo "  Response: $BODY"
    assert_status "DELETE /queues/${TOPIC}/messages" 200 "$HTTP_CODE" "$BODY"
    assert_json_field "ack status is OK" "status" "OK" "$BODY"
else
    echo -e "  ${YELLOW}⚠ SKIP${NC} No receiptHandle available (no message was received)"
fi

# =============================================================================
# Test 7: Ack with missing receiptHandle → 400
# =============================================================================
print_header "Test 7: Ack without receiptHandle → 400 Bad Request"

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X DELETE \
    -H "x-mq-request-id: curl-ack-002" \
    "${BASE_URL}/queues/${TOPIC}/messages?consumerGroup=${CONSUMER_GROUP}")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

assert_status "DELETE without receiptHandle" 400 "$HTTP_CODE" "$BODY"

# =============================================================================
# Test 8: Change invisible time (PUT) — only if we have a receiptHandle
# =============================================================================
print_header "Test 8: Change Invisible Time — PUT /queues/{topic}/messages"

if [ -n "$RECEIPT_HANDLE" ]; then
    ENCODED_HANDLE=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$RECEIPT_HANDLE'))" 2>/dev/null || echo "$RECEIPT_HANDLE")

    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X PUT \
        -H "x-mq-request-id: curl-change-001" \
        "${BASE_URL}/queues/${TOPIC}/messages?consumerGroup=${CONSUMER_GROUP}&receiptHandle=${ENCODED_HANDLE}&invisibleTime=60000")
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    BODY=$(echo "$RESPONSE" | head -1)

    echo "  Response: $BODY"
    assert_status "PUT /queues/${TOPIC}/messages" 200 "$HTTP_CODE" "$BODY"
else
    echo -e "  ${YELLOW}⚠ SKIP${NC} No receiptHandle available"
fi

# =============================================================================
# Test 9: Unsupported HTTP method → 405
# =============================================================================
print_header "Test 9: Unsupported HTTP Method → 405 Method Not Allowed"

RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X PATCH \
    -H "x-mq-request-id: curl-method-001" \
    "${BASE_URL}/queues/${TOPIC}/messages")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -1)

assert_status "PATCH /queues/${TOPIC}/messages" 405 "$HTTP_CODE" "$BODY"
assert_json_field "error code is MethodNotAllowed" "code" "MethodNotAllowed" "$BODY"

# =============================================================================
# Test 10: Send multiple messages in sequence
# =============================================================================
print_header "Test 10: Send 3 Messages in Sequence"

for i in 1 2 3; do
    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -H "x-mq-request-id: curl-batch-00${i}" \
        -d "{\"body\":\"Batch message ${i}\",\"tags\":\"BatchTag\",\"producerGroup\":\"${PRODUCER_GROUP}\"}" \
        "${BASE_URL}/queues/${TOPIC}/messages")
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    BODY=$(echo "$RESPONSE" | head -1)
    assert_status "Send batch message $i" 200 "$HTTP_CODE" "$BODY"
done

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${BLUE}══════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}══════════════════════════════════════════════════════${NC}"
TOTAL=$((PASS + FAIL))
echo -e "  Total:  $TOTAL"
echo -e "  ${GREEN}Passed: $PASS${NC}"
if [ "$FAIL" -gt 0 ]; then
    echo -e "  ${RED}Failed: $FAIL${NC}"
    echo ""
    exit 1
else
    echo -e "  ${GREEN}All tests passed! ✓${NC}"
    echo ""
    exit 0
fi
