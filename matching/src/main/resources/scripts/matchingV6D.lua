-- 주문 매칭 Lua 스크립트
-- KEYS[1]: 반대 주문 키 (매수면 SELL_ORDER_KEY, 매도면 BUY_ORDER_KEY)
-- KEYS[2]: 현재 주문 키 (매수면 BUY_ORDER_KEY, 매도면 SELL_ORDER_KEY)
-- KEYS[3]: 매칭 Stream 키 (v6d:stream:matches)
-- KEYS[4]: 미체결 Stream 키 (v6d:stream:unmatched)
-- KEYS[5]: 부분체결 Stream 키 (v6d:stream:partialMatched)
-- KEYS[6]: 멱등성 체크를 위한 키 (v6d:idempotency:orders)
-- KEYS[7]: 매수 호가 리스트 키 (v6d:orderbook:{tradingPair}:bids)
-- KEYS[8]: 매도 호가 리스트 키 (v6d:orderbook:{tradingPair}:asks)
-- KEYS[9]: 콜드 데이터 요청 스트림 키 (v6d:stream:cold_data_request)
-- KEYS[10]: 콜드 데이터 상태 키 (v6d:cold_data_status:{tradingPair})
-- KEYS[11]: 대기 주문 키 (v6d:pending_orders)
-- KEYS[12]: 일일 종가 키 (market:closing_price:{tradingPair})

-- ARGV[1]: 주문 타입 (BUY 또는 SELL)
-- ARGV[2]: 주문 가격
-- ARGV[3]: 주문 수량
-- ARGV[4]: 주문 상세 정보 (timestamp|quantity|userId|orderId 형식)
-- ARGV[5]: 거래 쌍 (trading pair)
-- ARGV[6]: 주문 ID
-- ARGV[7]: 부분 체결을 위한 ID
-- ARGV[8]: 가격 차이 임계값 (예: 30% = 0.3)

local oppositeOrderKey = KEYS[1]
local currentOrderKey = KEYS[2]
local matchStreamKey = KEYS[3]
local unmatchStreamKey = KEYS[4]
local partialMatchedStreamKey = KEYS[5]
local idempotencyKey = KEYS[6]
local bidOrderbookKey = KEYS[7]
local askOrderbookKey = KEYS[8]
local coldDataRequestStreamKey = KEYS[9]
local coldDataStatusKey = KEYS[10]
local pendingOrdersKey = KEYS[11]
local closingPriceKey = KEYS[12]

local orderType = ARGV[1]
local orderPrice = tonumber(ARGV[2])
local orderQuantity = tonumber(ARGV[3])
local orderDetails = ARGV[4]
local tradingPair = ARGV[5]
local orderId = ARGV[6]
local partialOrderId = ARGV[7]
local priceDiffThreshold = tonumber(ARGV[8])

-- 주문 정보 파싱 함수
local function parseOrderDetails(details)
    local pos = {}
    local val = {}
    local start = 1
    for i = 1, 3 do
        pos[i] = string.find(details, "|", start, true)
        val[i] = string.sub(details, start, pos[i] - 1)
        start = pos[i] + 1
    end
    val[4] = string.sub(details, start)
    return {
        timestamp = val[1],
        quantity = tonumber(val[2]),
        userId = val[3],
        orderId = val[4]
    }
end

-- 맵을 평면화하는 함수 (Redis XADD 명령에 사용)
local function flattenMap(map)
    local result = {}
    for k, v in pairs(map) do
        table.insert(result, k)
        table.insert(result, v)
    end
    return result
end

-- 주문 정보 구성 함수
local function buildOrderDetails(timestamp, quantity, userId, orderId)
    return timestamp .. "|" .. quantity .. "|" .. userId .. "|" .. orderId
end

-- 호가 업데이트 함수
local function updateOrderbook(price, quantity, isAdd, isAsk)
    local orderbookKey = isAsk and askOrderbookKey or bidOrderbookKey

    if isAdd then
        -- 호가 추가 또는 업데이트
        local currentQty = tonumber(redis.call("HGET", orderbookKey, tostring(price)) or "0")
        local newQty = currentQty + quantity

        if newQty > 0 then
            redis.call("HSET", orderbookKey, tostring(price), tostring(newQty))
        else
            -- 수량이 0이하면 해당 가격대 제거
            redis.call("HDEL", orderbookKey, tostring(price))
        end
    else
        -- 호가 감소
        local currentQty = tonumber(redis.call("HGET", orderbookKey, tostring(price)) or "0")
        local newQty = currentQty - quantity

        if newQty > 0 then
            redis.call("HSET", orderbookKey, tostring(price), tostring(newQty))
        else
            -- 수량이 0이하면 해당 가격대 제거
            redis.call("HDEL", orderbookKey, tostring(price))
        end
    end
end

-- 콜드 데이터 필요 여부 확인 함수
local function isColdDataNeeded(orderPrice)
    -- 가격 비교를 위한 종가 조회
    local closingPrice = tonumber(redis.call("GET", closingPriceKey) or "0")

    if closingPrice <= 0 then
        return false -- 종가 정보가 없으면 콜드 데이터 불필요
    end

    local priceDiff = math.abs(orderPrice - closingPrice) / closingPrice
    return priceDiff > priceDiffThreshold
end

-- 멱등성 체크: 이미 처리된 주문인지 확인
if redis.call("SISMEMBER", idempotencyKey, orderId) == 1 then
    -- 이미 처리된 주문이면 early return
    return true
end

local isBuy = orderType == "BUY"

-- 반대 주문 가져오기
local oppositeOrders = redis.call(isBuy and "ZRANGE" or "ZREVRANGE", oppositeOrderKey, 0, 0, "WITHSCORES")

-- 반대 주문이 없는 경우: 미체결 주문으로 처리
if #oppositeOrders == 0 then

    -- 콜드 데이터가 필요한지 확인
    if isColdDataNeeded(orderPrice) then
        -- 주문을 대기 큐에 추가
        redis.call("HSET", pendingOrdersKey, orderId,
                  orderDetails .. "|" .. orderPrice .. "|" .. orderType .. "|" .. tradingPair)

        local coldDataStatus = redis.call("GET", coldDataStatusKey)

        -- 콜드 데이터 처리 상태 확인
        if not coldDataStatus then  -- 아직 콜드 데이터를 요청한 적이 없는 경우
            -- 콜드 데이터 상태 업데이트
            redis.call("SET", coldDataStatusKey, "REQUESTED")

            -- 콜드 데이터 요청 스트림에 발행
            local requestFields = {
                ["tradingPair"] = tradingPair,
                ["orderType"] = isBuy and "SELL" or "BUY", -- 반대 주문 타입만 요청
                ["requestTime"] = tostring(redis.call("TIME")[1])
            }
            redis.call("XADD", coldDataRequestStreamKey, "MAXLEN", "~", 1000, "*", unpack(flattenMap(requestFields)))
        end
    else
        -- 현재 주문을 저장
        redis.call("ZADD", currentOrderKey, orderPrice, orderDetails)

        -- 주문 정보 파싱
        local orderInfo = parseOrderDetails(orderDetails)

        -- 호가 리스트 업데이트 - 새 주문 추가
        updateOrderbook(orderPrice, orderQuantity, true, not isBuy)

        -- 미체결 Stream에 발행
        local unmatchFields = {
            ["orderId"] = orderId,
            ["userId"] = orderInfo.userId,
            ["tradingPair"] = tradingPair,
            ["orderType"] = orderType,
            ["price"] = tostring(orderPrice),
            ["quantity"] = tostring(orderQuantity),
            ["timestamp"] = orderInfo.timestamp,
            ["operation"] = "INSERT"
        }
        redis.call("XADD", unmatchStreamKey, "MAXLEN", "~", 100000, "*", unpack(flattenMap(unmatchFields)))
    end

    -- 멱등성 키 추가
    redis.call("SADD", idempotencyKey, orderId)
    redis.call("EXPIRE", idempotencyKey, 86400)

    return true
end

-- 반대 주문 정보 처리
local oppositeOrderDetails = oppositeOrders[1]
local oppositeOrderPrice = tonumber(oppositeOrders[2])

-- 반대 주문과 매칭 가격 조건 확인
local isPriceMatched = isBuy and (orderPrice >= oppositeOrderPrice) or (not isBuy and orderPrice <= oppositeOrderPrice)
if not isPriceMatched then
    -- 현재 주문을 저장
    redis.call("ZADD", currentOrderKey, orderPrice, orderDetails)

    -- 호가 리스트 업데이트 - 새 주문 추가
    updateOrderbook(orderPrice, orderQuantity, true, not isBuy)

    -- 주문 정보 파싱
    local orderInfo = parseOrderDetails(orderDetails)

    -- 미체결 Stream에 발행
    local unmatchFields = {
        ["orderId"] = orderId,
        ["userId"] = orderInfo.userId,
        ["tradingPair"] = tradingPair,
        ["orderType"] = orderType,
        ["price"] = tostring(orderPrice),
        ["quantity"] = tostring(orderQuantity),
        ["timestamp"] = orderInfo.timestamp,
        ["operation"] = "INSERT"
    }
    redis.call("XADD", unmatchStreamKey, "MAXLEN", "~", 100000, "*", unpack(flattenMap(unmatchFields)))

    -- 멱등성 키추가
    redis.call("SADD", idempotencyKey, orderId)
    redis.call("EXPIRE", idempotencyKey, 86400)

    return true
end


-- 양쪽 주문 정보 파싱
local oppositeOrder = parseOrderDetails(oppositeOrderDetails)
local currentOrder = parseOrderDetails(orderDetails)

-- 매칭 가능한 수량 계산
local matchedQuantity = math.min(orderQuantity, oppositeOrder.quantity)
local remainingOppositeQuantity = oppositeOrder.quantity - matchedQuantity
local remainingOrderQuantity = orderQuantity - matchedQuantity

-- 매칭 가격 결정 (반대 주문의 가격)
local matchPrice = oppositeOrderPrice

-- 항상 먼저 반대 주문 정보 제거
redis.call("ZREM", oppositeOrderKey, oppositeOrderDetails)

-- 호가 리스트 업데이트 - 반대 주문 수량 감소
updateOrderbook(oppositeOrderPrice, matchedQuantity, false, isBuy)

-- 결과 관련 변수 초기화
local updatedOppositeDetails = ""
local updatedCurrentDetails = ""

-- 반대 주문 데이터 공통 필드
local orderOperation = remainingOppositeQuantity > 0 and "UPDATE" or "DELETE"
local orderQuantity = remainingOppositeQuantity > 0 and tostring(remainingOppositeQuantity) or "0"
local updateKey = "v6d:order:pending-updates:" .. oppositeOrder.orderId

-- 반대 주문 업데이트
if remainingOppositeQuantity > 0 then
    -- Sorted Set에 업데이트된 주문 추가
    updatedOppositeDetails = buildOrderDetails(
            oppositeOrder.timestamp,
            remainingOppositeQuantity,
            oppositeOrder.userId,
            oppositeOrder.orderId
    )
    redis.call("ZADD", oppositeOrderKey, oppositeOrderPrice, updatedOppositeDetails)
end

-- 현재 버전 정보 확인 (없으면 기본값 1 사용)
local currentVersion = tonumber(redis.call("HGET", updateKey, "version") or "1")
local nextVersion = currentVersion + 1

-- 변경 수량을 DB에 업데이트하기 위한 해시맵 생성
redis.call("HSET", updateKey,
    "orderId", oppositeOrder.orderId,
    "userId", oppositeOrder.userId,
    "tradingPair", tradingPair,
    "orderType", isBuy and "SELL" or "BUY",
    "price", tostring(oppositeOrderPrice),
    "quantity", orderQuantity,
    "timestamp", oppositeOrder.timestamp,
    "operation", orderOperation,
    "version", tostring(nextVersion)
)

-- 해시셋 전용 인덱스에 추가
redis.call("SADD", "v6d:order:pending-updates:index", oppositeOrder.orderId)

-- 현재 주문에 남은 수량이 있는 경우
if remainingOrderQuantity > 0 then
    updatedCurrentDetails = buildOrderDetails(
            currentOrder.timestamp,
            remainingOrderQuantity,
            currentOrder.userId,
            currentOrder.orderId
    )

    -- 남은 수량을 미체결 Stream에 발행
    local partialMatchedFields = {
        ["orderId"] = partialOrderId,
        ["userId"] = currentOrder.userId,
        ["tradingPair"] = tradingPair,
        ["orderType"] = orderType,
        ["price"] = tostring(orderPrice),
        ["quantity"] = tostring(remainingOrderQuantity),
        ["timestamp"] = currentOrder.timestamp,
    }
    redis.call("XADD", partialMatchedStreamKey, "MAXLEN", "~", 100000, "*", unpack(flattenMap(partialMatchedFields)))
end

-- 매칭 결과를 Stream에 발행
local buyOrder = isBuy and currentOrder or oppositeOrder
local sellOrder = isBuy and oppositeOrder or currentOrder

local matchFields = {
    ["buyOrderId"] = buyOrder.orderId,
    ["sellOrderId"] = sellOrder.orderId,
    ["buyUserId"] = buyOrder.userId,
    ["sellUserId"] = sellOrder.userId,
    ["tradingPair"] = tradingPair,
    ["executionPrice"] = tostring(matchPrice),
    ["matchedQuantity"] = tostring(matchedQuantity),
    ["timestamp"] = tostring(redis.call("TIME")[1])
}

-- Redis Stream에 매칭 정보 추가
redis.call("XADD", matchStreamKey, "MAXLEN", "~", 100000, "*", unpack(flattenMap(matchFields)))

-- 멱등성 키추가
redis.call("SADD", idempotencyKey, orderId)
redis.call("EXPIRE", idempotencyKey, 86400)

-- 결과 데이터 (필수 정보만 단순한 리스트로 반환)
return true