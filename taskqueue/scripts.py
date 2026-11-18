"""Lua scripts that make every state transition of a job atomic.

Each script runs as a single Redis command, so a job can never be "popped but not
leased": either it is still in the ready set or it is in the in-flight set with a lease
deadline. That property is what gives at-least-once delivery when a worker crashes.

Ownership check: a worker acks with the attempt number it received. If the lease expired
and the job was handed to another worker, the attempt counter has moved on and the stale
ack is ignored.
"""

# KEYS: 1 ready, 2 delayed, 3 in-flight
# ARGV: 1 now_ms, 2 count, 3 lease_ms, 4 worker_id, 5 job key prefix, 6 max priority, 7 priority stride
# Returns: list of flat HGETALL replies for the leased jobs.
DEQUEUE = """
local now = tonumber(ARGV[1])
local count = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])
local prefix = ARGV[5]
local maxp = tonumber(ARGV[6])
local stride = tonumber(ARGV[7])

-- Promote delayed jobs (initial delay or retry back-off) whose time has come.
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, 1000)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[2], id)
  local p = redis.call('HGET', prefix .. id, 'priority')
  if p then
    local score = (maxp - tonumber(p)) * stride + now
    redis.call('ZADD', KEYS[1], string.format('%.0f', score), id)
    redis.call('HSET', prefix .. id, 'status', 'queued')
  end
end

-- Lease up to `count` jobs in priority order, in one round trip.
local out = {}
local ids = redis.call('ZRANGE', KEYS[1], 0, count - 1)
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  local key = prefix .. id
  if redis.call('EXISTS', key) == 1 then
    redis.call('ZADD', KEYS[3], string.format('%.0f', now + lease), id)
    redis.call('HINCRBY', key, 'attempts', 1)
    redis.call('HSET', key, 'status', 'running', 'started_at', ARGV[1], 'worker_id', ARGV[4])
    out[#out + 1] = redis.call('HGETALL', key)
  end
end
return out
"""

# KEYS: 1 in-flight, 2 job, 3 stats, 4 completed index, 5 latency samples, 6 throughput bucket
# ARGV: 1 id, 2 attempt, 3 now_ms, 4 result json, 5 job ttl s, 6 completed index size,
#       7 bucket ttl s, 8 latency sample count
# Returns 1 on success, 0 if this ack is stale (lease lost).
COMPLETE = """
if redis.call('HGET', KEYS[2], 'attempts') ~= ARGV[2] or not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'status', 'completed', 'finished_at', ARGV[3], 'result', ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[5])
redis.call('HINCRBY', KEYS[3], 'completed', 1)

redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1])
redis.call('ZREMRANGEBYRANK', KEYS[4], 0, -(tonumber(ARGV[6]) + 1))

local run_at = tonumber(redis.call('HGET', KEYS[2], 'run_at'))
redis.call('LPUSH', KEYS[5], tonumber(ARGV[3]) - run_at)
redis.call('LTRIM', KEYS[5], 0, tonumber(ARGV[8]) - 1)

redis.call('INCR', KEYS[6])
redis.call('EXPIRE', KEYS[6], ARGV[7])
return 1
"""

# KEYS: 1 in-flight, 2 delayed, 3 dead, 4 job, 5 stats
# ARGV: 1 id, 2 attempt, 3 now_ms, 4 error, 5 retry_at_ms (-1 => dead-letter), 6 only_if_expired (1/0)
# Returns 1 = scheduled for retry, 2 = dead-lettered, 0 = stale / lease still valid.
FAIL = """
local lease = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not lease or redis.call('HGET', KEYS[4], 'attempts') ~= ARGV[2] then
  return 0
end
if ARGV[6] == '1' and tonumber(lease) > tonumber(ARGV[3]) then
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HINCRBY', KEYS[5], 'failed', 1)
redis.call('HSET', KEYS[4], 'last_error', ARGV[4])
if tonumber(ARGV[5]) >= 0 then
  redis.call('ZADD', KEYS[2], ARGV[5], ARGV[1])
  redis.call('HSET', KEYS[4], 'status', 'scheduled', 'retry_at', ARGV[5])
  redis.call('HINCRBY', KEYS[5], 'retried', 1)
  return 1
end
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
redis.call('HSET', KEYS[4], 'status', 'dead', 'finished_at', ARGV[3])
redis.call('HINCRBY', KEYS[5], 'dead', 1)
return 2
"""

# KEYS: 1 in-flight, 2 job   ARGV: 1 id, 2 attempt, 3 new deadline ms
EXTEND = """
if redis.call('HGET', KEYS[2], 'attempts') ~= ARGV[2] or not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1
"""

# KEYS: 1 dead, 2 ready, 3 job   ARGV: 1 id, 2 ready score, 3 now_ms
REQUEUE_DEAD = """
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[3], 'status', 'queued', 'attempts', '0', 'run_at', ARGV[3])
redis.call('HDEL', KEYS[3], 'finished_at', 'retry_at')
return 1
"""
