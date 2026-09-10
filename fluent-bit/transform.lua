function transform_log(tag, timestamp, record)
    local level = "INFO"
    if record["status"] >= 500 then
        level = "ERROR"
    elseif record["status"] >= 400 then
        level = "WARN"
    end

    local new_record = {}
    new_record["timestamp"] = record["timestamp"]
    new_record["service"] = "nginx"
    new_record["level"] = level
    new_record["message"] = record["request_method"] .. " " .. record["request_uri"]
    new_record["http_code"] = record["status"]
    new_record["latency_ms"] = record["request_time"] * 1000
    new_record["ip"] = record["remote_addr"]

    return 1, timestamp, new_record
end
