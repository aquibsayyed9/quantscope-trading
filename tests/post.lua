-- wrk.method = "POST"
-- wrk.body =
-- 	'{"symbol":"BTCUSD","side":"BUY","price":50000,"quantity":0.1,"order_type":"LIMIT","time_in_force":"GTC","user_id":"loadtest"}'
-- wrk.headers["Content-Type"] = "application/json"

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

local counter = 0

request = function()
	counter = counter + 1

	local side = math.random() > 0.5 and "BUY" or "SELL"
	local order_type = "LIMIT"
	local time_in_force = "GTC"
	local price = math.random(49000, 51000)
	local quantity = math.random(10, 100) / 10.0

	-- 10% market orders
	if math.random() < 0.1 then
		order_type = "MARKET"
		price = 0
	end

	-- 20% IOC orders
	if math.random() < 0.2 then
		time_in_force = "IOC"
	end

	local body = string.format(
		[[{
        "symbol": "BTCUSD",
        "side": "%s",
        "price": %d,
        "quantity": %.1f,
        "order_type": "%s",
        "time_in_force": "%s",
        "user_id": "user_%d"
    }]],
		side,
		price,
		quantity,
		order_type,
		time_in_force,
		math.random(1, 1000)
	)
	return wrk.format("POST", nil, nil, body)
end
