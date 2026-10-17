"""Atomic, replay-safe cache projection. PostgreSQL remains authoritative."""
import json

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
for _, key in ipairs(counters) do redis.call('INCR', key) end
if not previous or previous < ARGV[2] then
  redis.call('SET', KEYS[2] .. ':version', ARGV[2])
  redis.call('SET', KEYS[2], ARGV[3], 'EX', 86400)
end
if ARGV[4] ~= '' then
  redis.call('LPUSH', ARGV[4], ARGV[3])
  redis.call('LTRIM', ARGV[4], 0, 50)
end
redis.call('SET', KEYS[1], '1')
return 1
'''


def cache_once(client, namespace, key, latest_key, timestamp, payload, counters, list_key=""):
    client.eval(SCRIPT, 2, f"pipeline:cache:{namespace}:{key}", latest_key,
                json.dumps(counters), timestamp + ':' + key, json.dumps(payload, default=str), list_key)
