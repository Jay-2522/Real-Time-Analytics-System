"""Atomic, replay-safe cache projection. PostgreSQL remains authoritative."""
import json
from pipeline_runtime.cache_recovery import encode

SCRIPT = '''
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local counters = cjson.decode(ARGV[1])
for _, key in ipairs(counters) do
  local value = redis.call('GET', key)
  if value and (not string.match(value, '^%-?%d+$') or math.abs(tonumber(value)) > 9000000000000000) then
    return redis.error_reply('Invalid cache counter')
  end
end
local previous = redis.call('GET', KEYS[2] .. ':version')
if ARGV[4] ~= '' then
  local kind = redis.call('TYPE', ARGV[4]).ok
  if kind ~= 'none' and kind ~= 'list' then return redis.error_reply('Invalid alert list') end
end
local items = {}
if ARGV[4] ~= '' then
  items = redis.call('LRANGE', ARGV[4], 0, 50)
  table.insert(items, ARGV[3])
  table.sort(items, function(a, b)
    local x, y = cjson.decode(a), cjson.decode(b)
    local xt = string.gsub(x.timestamp, 'Z$', '+00:00')
    local yt = string.gsub(y.timestamp, 'Z$', '+00:00')
    return xt .. ':' .. x.alert_id > yt .. ':' .. y.alert_id
  end)
end
for _, key in ipairs(counters) do redis.call('INCR', key) end
if not previous or previous < ARGV[2] then
  redis.call('SET', KEYS[2] .. ':version', ARGV[2])
  if tonumber(ARGV[5]) > 0 then
    redis.call('SET', KEYS[2], ARGV[3], 'EXAT', ARGV[5])
  end
end
if ARGV[4] ~= '' then
  redis.call('DEL', ARGV[4])
  for i = math.min(#items, 51), 1, -1 do redis.call('LPUSH', ARGV[4], items[i]) end
end
redis.call('SET', KEYS[1], '1')
return 1
'''


def cache_once(client, namespace, key, latest_key, timestamp, payload, counters, list_key="", expires_at=None):
    import time
    expiry = int(expires_at if expires_at is not None else time.time() + 86400)
    client.eval(SCRIPT, 2, f"pipeline:cache:{namespace}:{key}", latest_key,
                json.dumps(counters), timestamp + ':' + key, encode(payload), list_key, expiry)
