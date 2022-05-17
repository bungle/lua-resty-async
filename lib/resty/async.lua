---
-- A thread pool based asynchronous job scheduler for OpenResty.
--
-- @module resty.async


local semaphore = require "ngx.semaphore"


local ngx = ngx
local log = ngx.log
local now = ngx.now
local wait = ngx.thread.wait
local kill = ngx.thread.kill
local sleep = ngx.sleep
local spawn = ngx.thread.spawn
local exiting = ngx.worker.exiting
local timer_at = ngx.timer.at
local fmt = string.format
local sort = table.sort
local math = math
local min = math.min
local max = math.max
local huge = math.huge
local ceil = math.ceil
local floor = math.floor
local type = type
local traceback = debug.traceback
local xpcall = xpcall
local select = select
local unpack = unpack
local assert = assert
local setmetatable = setmetatable


local DEBUG  = ngx.DEBUG
local INFO   = ngx.INFO
local NOTICE = ngx.NOTICE
local ERR    = ngx.ERR
local WARN   = ngx.WARN
local CRIT   = ngx.CRIT


local TIMER_INTERVAL = 0.1
local WAIT_INTERVAL  = 0.5
local LOG_STEP       = 0.5
local LOG_INTERVAL   = 60
local THREADS        = 100
local RESPAWN_LIMIT  = 1000
local BUCKET_SIZE    = 1000
local LAWN_SIZE      = 10000
local QUEUE_SIZE     = 100000


local DELAYS = {
  second = 1,
  minute = 60,
  hour   = 3600,
  day    = 86400,
  week   = 604800,
  month  = 2629743.833,
  year   = 31556926,
}


local MONKEY_PATCHED


local function get_pending(queue, size)
  local head = queue.head
  local tail = queue.tail
  if head < tail then
    head = head + size
  end
  return head - tail
end


local function is_full(queue, size)
  return get_pending(queue, size) == size
end


local new_tab
do
  local ok
  ok, new_tab = pcall(require, "table.new")
  if not ok then
    new_tab = function ()
      return {}
    end
  end
end


local function release_timer(self)
  local timers = self.timers
  if timers > 0 then
    timers = timers - 1
  end

  if timers > 0 then
    self.timers = timers
  else
    self.timers = 0
    self.quit:post()
  end
end


local function job_thread(self, index)
  local wait_interval = self.opts.wait_interval
  local queue_size = self.opts.queue_size
  local respawn_limit = self.opts.respawn_limit
  local jobs_executed = 0

  while true do
    local ok, err = self.work:wait(wait_interval)
    if ok then
      local tail = self.tail == queue_size and 1 or self.tail + 1
      local job = self.jobs[tail]
      self.tail = tail
      self.jobs[tail] = nil
      self.running = self.running + 1
      self.time[tail][2] = now() * 1000
      ok, err = job()
      self.time[tail][3] = now() * 1000
      self.running = self.running - 1
      self.done = self.done + 1
      if not ok then
        self.errored = self.errored + 1
        log(ERR, "async thread #", index, " job error: ", err)
      end

      jobs_executed = jobs_executed + 1

    elseif err ~= "timeout" then
      log(ERR, "async thread #", index, " wait error: ", err)
    end

    if self.head == self.tail and (self.started == nil or self.aborted > 0 or exiting()) then
      break
    end

    if jobs_executed == respawn_limit then
      return index, true
    end
  end

  return index
end


local function job_timer(premature, self)
  if premature or self.started == nil then
    release_timer(self)
    return true
  end

  local t = self.threads
  local c = self.opts.threads

  for i = 1, c do
    t[i] = spawn(job_thread, self, i)
  end

  while true do
    local ok, err, respawn = wait(unpack(t, 1, c))
    if respawn then
      log(DEBUG, "async respawning thread #", err)
      t[err] = spawn(job_thread, self, err)

    else
      if not ok then
        log(ERR, "async thread error: ", err)
      elseif self.started and not exiting() then
        log(ERR, "async thread #", err, " aborted")
      end

      self.aborted = self.aborted + 1

      break
    end
  end

  for i = 1, c do
    local ok, err = wait(t[i])
    if not ok then
      log(ERR, "async thread error: ", err)
    elseif self.started and not exiting() then
      log(ERR, "async thread #", i, " aborted")
    end

    kill(t[i])
    t[i] = nil

    self.aborted = self.aborted + 1
  end

  release_timer(self)

  if self.started and not exiting() then
    log(CRIT, "async job timer aborted")
    return
  end

  return true
end


local function queue_timer(premature, self)

  -- DO NOT YIELD IN THIS FUNCTION AS IT IS EXECUTED FREQUENTLY!

  local timer_interval = self.opts.timer_interval
  local lawn_size      = self.opts.lawn_size
  local bucket_size    = self.opts.bucket_size

  while true do
    premature = premature or self.started == nil or exiting()

    local ok = true

    if self.lawn.head ~= self.lawn.tail then
      local current_time = premature and huge or now()
      if self.closest <= current_time then
        local lawn    = self.lawn
        local ttls    = lawn.ttls
        local buckets = lawn.buckets
        local head    = lawn.head
        local tail    = lawn.tail
        while head ~= tail do
          tail = tail == lawn_size and 1 or tail + 1
          local ttl = ttls[tail]
          local bucket = buckets[ttl]
          if bucket.head == bucket.tail then
            lawn.tail = lawn.tail == lawn_size and 1 or lawn.tail + 1
            buckets[ttl] = nil
            ttls[tail] = nil

          else
            while bucket.head ~= bucket.tail do
              local bucket_tail = bucket.tail == bucket_size and 1 or bucket.tail + 1
              local job = bucket.jobs[bucket_tail]
              local expiry = job[1]
              if expiry >= current_time then
                break
              end

              local err
              ok, err = job[2](self)
              if not ok then
                log(ERR, err)
                break
              end

              bucket.jobs[bucket_tail] = nil
              bucket.tail = bucket_tail

              -- reschedule a recurring job
              if not premature and job[3] then
                local exp = now() + ttl
                bucket.head = bucket.head == bucket_size and 1 or bucket.head + 1
                bucket.jobs[bucket.head] = {
                  exp,
                  job[2],
                  true,
                }

                self.closest = min(self.closest, exp)
              end

              if self.closest == 0 or self.closest > expiry then
                self.closest = expiry
              end
            end

            lawn.tail = lawn.tail == lawn_size and 1 or lawn.tail + 1
            lawn.head = lawn.head == lawn_size and 1 or lawn.head + 1
            ttls[lawn.head] = ttl
            ttls[tail] = nil

            if not ok then
              break
            end
          end
        end
      end
    end

    if premature then
      break
    end

    sleep(timer_interval)
  end

  release_timer(self)

  if self.started and not exiting() then
    log(CRIT, "async queue timer aborted")
    return
  end

  return true
end


local function log_timer(premature, self)
  if premature or self.started == nil then
    release_timer(self)
    return true
  end

  local log_interval = self.opts.log_interval
  local log_step     = self.opts.log_step
  local queue_size   = self.opts.queue_size

  local round  = 0
  local rounds = ceil(log_interval / log_step)

  while true do
    if self.started == nil or exiting() then
      break
    end

    round = round + 1

    if round == rounds then
      round = 0

      local dbg = queue_size / 10000
      local nfo = queue_size / 1000
      local ntc = queue_size / 100
      local wrn = queue_size / 10
      local err = queue_size

      local pending = get_pending(self, queue_size)

      local msg = fmt("async jobs: %u running, %u pending, %u errored, %u refused, %u aborted, %u done",
        self.running, pending, self.errored, self.refused, self.aborted, self.done)
      if pending <= dbg then
        log(DEBUG, msg)
      elseif pending <= nfo then
        log(INFO, msg)
      elseif pending <= ntc then
        log(NOTICE, msg)
      elseif pending < wrn then
        log(WARN, msg)
      elseif pending < err then
        log(ERR, msg)
      else
        log(CRIT, msg)
      end
    end

    sleep(log_step)
  end

  release_timer(self)

  if self.started and not exiting() then
    log(CRIT, "async log timer aborted")
    return
  end

  return true
end


local function create_job(func, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, ...)
  local argc = select("#", ...)
  if argc == 0 then
    return function()
      return xpcall(func, traceback, exiting(),
                    a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
    end
  end

  local args = { ... }
  return function()
    local pok, res, err = xpcall(func, traceback, exiting(),
                                 a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
                                 unpack(args, 1, argc))
    if not pok then
      return nil, res
    end

    if not err then
      return true
    end

    return nil, err
  end
end


local function queue_job(self, job)
  local queue_size = self.opts.queue_size
  if is_full(self, queue_size) then
    self.refused = self.refused + 1
    return nil, "async queue is full"
  end

  self.head = self.head == queue_size and 1 or self.head + 1
  self.jobs[self.head] = job
  self.time[self.head][1] = now() * 1000
  self.work:post()

  return true
end


local function create_recurring_job(job)
  local running = false

  local recurring_job = function()
    running = true
    local ok, err = job()
    running = false
    return ok, err
  end

  return function(self)
    if running then
      return nil, "recurring job is already running"
    end

    return queue_job(self, recurring_job)
  end
end


local function create_at_job(job)
  return function(self)
    return queue_job(self, job)
  end
end


local function get_stats(self, from, to)
  local data, size = self:data(from, to)
  local names = { "max", "min", "mean", "median", "p95", "p99", "p999" }
  local stats = new_tab(2, 0)

  for i = 1, 2 do
    stats[i] = new_tab(0, #names + 1)
    stats[i].size = size
    if size == 0 then
      for j = 1, #names do
        stats[i][names[j]] = 0
      end

    elseif size == 1 then
      local time = i == 1 and data[1][2] - data[1][1]
                           or data[1][3] - data[1][2]

      for j = 1, #names do
        stats[i][names[j]] = time
      end

    elseif size > 1 then
      local tot = 0
      local raw = new_tab(size, 0)
      local max_value
      local min_value

      for j = 1, size do
        local time = i == 1 and data[j][2] - data[j][1]
                             or data[j][3] - data[j][2]
        raw[j] = time
        tot = tot + time
        max_value = max(time, max_value or time)
        min_value = min(time, min_value or time)
      end

      stats[i].max = max_value
      stats[i].min = min_value
      stats[i].mean = floor(tot / size + 0.5)

      sort(raw)

      local n = { "median", "p95", "p99", "p999" }
      local m = { 0.5, 0.95, 0.99, 0.999 }

      for j = 1, #n do
        local idx = size * m[j]
        if idx == floor(idx) then
          stats[i][n[j]] = floor((raw[idx] + raw[idx + 1]) / 2 + 0.5)
        else
          stats[i][n[j]] = raw[floor(idx + 0.5)]
        end
      end
    end
  end

  return {
    latency = stats[1],
    runtime = stats[2],
  }
end


local function schedule(self, delay, recurring, func, ...)
  if not self.started then
    return nil, "async not started"
  end

  if exiting() then
    return nil, "nginx worker exiting"
  end

  delay = DELAYS[delay] or delay

  if recurring then
    assert(type(delay) == "number" and delay > 0, "invalid delay, must be a number greater than zero or " ..
                                                  "'second', 'minute', 'hour', 'month' or 'year'")
  else
    assert(type(delay) == "number" and delay >= 0, "invalid delay, must be a positive number, zero or " ..
                                                   "'second', 'minute', 'hour', 'month' or 'year'")

    if delay == 0 then
      return queue_job(self, create_job(func, ...))
    end
  end

  local lawn = self.lawn
  local lawn_size = self.opts.lawn_size
  local bucket_size = self.opts.bucket_size
  if is_full(lawn, lawn_size) then
    self.refused = self.refused + 1
    return nil, "async lawn (" .. delay .. ") is full"
  end

  local bucket = lawn.buckets[delay]
  if bucket then
    if is_full(bucket, bucket_size) then
      self.refused = self.refused + 1
      return nil, "async bucket (" .. delay .. ") is full"
    end

  else
    lawn.head = lawn.head == lawn_size and 1 or lawn.head + 1
    lawn.ttls[lawn.head] = delay

    bucket = {
      jobs = new_tab(bucket_size, 0),
      head = 0,
      tail = 0,
    }

    lawn.buckets[delay] = bucket
  end

  local job = create_job(func, ...)
  if recurring then
    job = create_recurring_job(job)
  else
    job = create_at_job(job)
  end

  local expiry = now() + delay

  bucket.head = bucket.head == bucket_size and 1 or bucket.head + 1
  bucket.jobs[bucket.head] = {
    expiry,
    job,
    recurring
  }

  self.closest = min(self.closest, expiry)

  return true
end


local async = {
  _VERSION = "0.1"
}

async.__index = async


---
-- Creates a new instance of `resty.async`
--
-- @tparam[opt]  table  options  a table containing options
--
-- The following options can be used:
--
-- - `timer_interval` (the default is `0.1`)
-- - `wait_interval`  (the default is `0.5`)
-- - `log_step`       (the default is `0.5`)
-- - `log_interval`   (the default is `60`)
-- - `threads`        (the default is `100`)
-- - `respawn_limit`  (the default is `1000`)
-- - `bucket_size`    (the default is `1000`)
-- - `lawn_size`      (the default is `10000`)
-- - `queue_size`     (the default is `100000`)
--
-- @treturn      table           an instance of `resty.async`
function async.new(options)
  assert(options == nil or type(options) == "table", "invalid options")

  local opts = {
    timer_interval = options and options.timer_interval or TIMER_INTERVAL,
    wait_interval  = options and options.wait_interval  or WAIT_INTERVAL,
    log_interval   = options and options.log_interval   or LOG_INTERVAL,
    log_step       = options and options.log_step       or LOG_STEP,
    threads        = options and options.threads        or THREADS,
    respawn_limit  = options and options.respawn_limit  or RESPAWN_LIMIT,
    bucket_size    = options and options.bucket_size    or BUCKET_SIZE,
    lawn_size      = options and options.lawn_size      or LAWN_SIZE,
    queue_size     = options and options.query_size     or QUEUE_SIZE,
  }

  local time = new_tab(opts.queue_size, 0)
  for i = 1, opts.queue_size do
    time[i] = new_tab(3, 0)
  end

  return setmetatable({
    opts = opts,
    jobs = new_tab(opts.queue_size, 0),
    time = time,
    quit = semaphore.new(),
    work = semaphore.new(),
    lawn = {
      head    = 0,
      tail    = 0,
      ttls    = new_tab(opts.lawn_size, 0),
      buckets = {},
    },
    groups = {},
    timers = 0,
    threads = new_tab(opts.threads, 0),
    closest = 0,
    running = 0,
    errored = 0,
    refused = 0,
    aborted = 0,
    done = 0,
    head = 0,
    tail = 0,
  }, async)
end


---
-- Start `resty.async` timers
--
-- @treturn boolean|nil `true` on success, `nil` on error
-- @treturn string|nil  `nil` on success, error message `string` on error
function async:start()
  if self.started then
    return nil, "async already started"
  end

  if exiting() then
    return nil, "nginx worker exiting"
  end

  self.started = now()
  self.aborted = 0

  local ok, err = timer_at(0, job_timer, self)
  if not ok then
    return nil, err
  end

  self.timers = self.timers + 1

  ok, err = timer_at(self.opts.timer_interval, queue_timer, self)
  if not ok then
    return nil, err
  end

  self.timers = self.timers + 1

  if self.opts.log_interval > 0 then
    ok, err = timer_at(self.opts.log_step, log_timer, self)
    if not ok then
      return nil, err
    end

    self.timers = self.timers + 1
  end

  return true
end


---
-- Stop `resty.async` timers
--
-- @tparam[opt] number       wait  specifies the maximum time this function call should wait for
-- (in seconds) everything to be stopped
-- @treturn     boolean|nil        `true` on success, `nil` on error
-- @treturn     string|nil         `nil` on success, error message `string` on error
function async:stop(wait)
  if not self.started then
    return nil, "async not started"
  end

  if exiting() then
    return nil, "nginx worker exiting"
  end

  self.started = nil

  if wait then
    return self.quit:wait(wait)
  end

  return true
end


---
-- Run a function asynchronously
--
-- @tparam       function    func  a function to run asynchronously
-- @tparam[opt]  ...         ...   function arguments
-- @treturn      true|nil          `true` on success, `nil` on error
-- @treturn      string|nil        `nil` on success, error message `string` on error
function async:run(func, ...)
  if not self.started then
    return nil, "async not started"
  end

  if exiting() then
    return nil, "nginx worker exiting"
  end

  return queue_job(self, create_job(func, ...))
end


---
-- Run a function asynchronously and repeatedly but non-overlapping
--
-- @tparam       number|string  delay  function execution interval (a non-zero positive number
-- or `"second"`, `"minute"`, `"hour"`, `"month" or `"year"`)
-- @tparam       function       func   a function to run asynchronously
-- @tparam[opt]  ...            ...    function arguments
-- @treturn      true|nil              `true` on success, `nil` on error
-- @treturn      string|nil            `nil` on success, error message `string` on error
function async:every(delay, func, ...)
  return schedule(self, delay, true, func, ...)
end


---
-- Run a function asynchronously with a specific delay
--
-- @tparam       number|string  delay  function execution delay (a positive number, zero included,
-- or `"second"`, `"minute"`, `"hour"`, `"month" or `"year"`)
-- @tparam       function       func   a function to run asynchronously
-- @tparam[opt]  ...            ...    function arguments
-- @treturn      true|nil              `true` on success, `nil` on error
-- @treturn      string|nil            `nil` on success, error message `string` on error
function async:at(delay, func, ...)
  return schedule(self, delay, false, func, ...)
end


---
-- Return raw metrics
--
-- @tparam[opt]  number  from  data start time (from unix epoch)
-- @tparam[opt]  number  to    data end time (from unix epoch)
-- @treturn      table         a table containing the metrics
-- @treturn      number        number of metrics returned
function async:data(from, to)
  local time = self.time
  local done = min(self.done, self.opts.queue_size)
  if not from and not to then
    return time, done
  end

  from = from and from * 1000 or 0
  to   = to   and to   * 1000 or huge

  local data = new_tab(done, 0)
  local size = 0
  for i = 1, done do
    if time[i][1] >= from and time[i][3] <= to then
      size = size + 1
      data[size] = time[i]
    end
  end

  return data, size
end


---
-- Return statistics
--
-- @tparam[opt]  table  options  the options of which statistics to calculate
--
-- The following options can be used:
--
-- - `all`    (boolean): calculate statistics for all the executed jobs
-- - `hour`   (boolean): calculate statistics for the last hour
-- - `minute` (boolean): calculate statistics for the last minute
--
-- @treturn      table        a table containing calculated statistics
function async:stats(options)
  local stats
  local pending = get_pending(self, self.opts.queue_size)
  if not options then
    stats = get_stats(self)
    stats.done    = self.done
    stats.pending = pending
    stats.running = self.running
    stats.errored = self.errored
    stats.refused = self.refused
    stats.aborted = self.aborted

  else
    local current_time = now()

    local all    = options.all    and get_stats(self)
    local minute = options.minute and get_stats(self, current_time - DELAYS.minute)
    local hour   = options.hour   and get_stats(self, current_time - DELAYS.hour)

    local latency
    local runtime
    if all or minute or hour then
      latency = {
        all    = all    and all.latency,
        minute = minute and minute.latency,
        hour   = hour   and hour.latency,
      }
      runtime = {
        all    = all    and all.runtime,
        minute = minute and minute.runtime,
        hour   = hour   and hour.runtime,
      }
    end

    stats = {
      done    = self.done,
      pending = pending,
      running = self.running,
      errored = self.errored,
      refused = self.refused,
      aborted = self.aborted,
      latency = latency,
      runtime = runtime,
    }
  end

  return stats
end


---
-- Monkey patches `ngx.timer.*` APIs with `resty.async` APIs
function async:patch()
  if MONKEY_PATCHED then
    if self.ngx and self.ngx.timer then
      return nil, "already monkey patched"
    end

    return nil, "already monkey patched by other instance"
  end

  MONKEY_PATCHED = true

  self.ngx = {
    timer = {
      at = ngx.timer.at,
      every = ngx.timer.every,
    }
  }

  ngx.timer.at = function(...)
    return self:at(...)
  end

  ngx.timer.every = function(...)
    return self:every(...)
  end

  return true
end


---
-- Removes monkey patches from `ngx.timer.*` APIs
function async:unpatch()
  if not MONKEY_PATCHED then
    return nil, "not monkey patched"
  end

  if not self.ngx or not self.ngx.timer then
    return nil, "patched by other instance"
  end

  MONKEY_PATCHED = nil

  local timer = self.ngx.timer
  if timer.at then
    ngx.timer.at = timer.at
  end

  if timer.every then
    ngx.timer.every = timer.every
  end

  self.ngx = nil

  return true
end


return async
