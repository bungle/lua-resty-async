# lua-resty-async

A thread pool based asynchronous job scheduler for OpenResty.


## API


### Functions and Methods


#### local async = async.new(options)

Creates a new instance of `resty.async`. With the `options` argument you can control the behavior
and the limits:

- `timer_interval`: how ofter the queue timer runs in seconds (the default is `0.1`)
- `wait_interval`: how long the light threads wait for a job on each step, in seconds (the default is `0.5`)
- `log_step`: how long the log timer sleeps between the steps, in seconds (the default is `0.5`)
- `log_interval`: how often the log timer writes to the log, in seconds (the default is `60`, `0` disables the log timer)
- `threads`: how many light threads to start that execute the jobs (the default is `100`)
- `respawn_limit`: how many jobs a light thread can execute, before it is respawned (the default is `1000`)
- `bucket_size`: how many jobs can be put in each bucket (each interval has its own bucket), buckets are drained by queue timer (the default is `1000`)
- `lawn_size`: how many jobs can be added to buckets (combined) before we don't accept new jobs, again these are drained as the jobs are queued (the default is `10000`)
- `queue_size`: how many jobs can be queued for the light threads concurrently, this is also drained as jobs are executed by the light threads (the default is `100000`)


#### local ok, err = async:start()

Starts `resty.async` timers. Returns `true` on success, otherwise return `nil` and error message.

Possible errors include:
- It was already started
- Nginx worker is exiting
- Starting timer jobs fail


#### local ok, err = async:stop(wait)

Stops `resty.async` timers. Returns `true` on success, otherwise return `nil` and error message.
You can pass the optional `wait` argument (`number`) to wait for stop to stop the timers.

When stopping the `async`, it will still execute all the queued jobs before it stops (it still
passes `premature=true` to them).

Possible errors include:
- It was not started before calling stop
- Nginx worker is exiting
- Waiting fails (e.g. a timeout) when using the optional `wait` argument


#### local ok, err = async:run(func, ...)

Queues a job right away and skips the scheduling through queue timer. This is same as calling
`async:at(0, func, ...)`, that is with `0` delay. It will immediately release a semaphore that
any of the threads in thread pool can pick up and start executing as soon as Nginx scheduler
gives CPU to it.

The possible errors
- Async is not yet started (this might change, if we want to enable queuing in `init` phase, for example)
- Nginx worker is exiting
- Async queue is full


#### local ok, err = async:every(delay, func, ...)

Queues a recurring job that runs approximately every `delay` seconds. This is similar to
[`ngx.timer.every`](https://github.com/openresty/lua-nginx-module#ngxtimerevery), but instead
of executing jobs with a timer construct we use a thread pool of light threads to execute it.
The job is never queued overlapping, if it takes more than `delay` to execute a job.

Possible errors:
- Async is not yet started (this might change, if we want to enable queuing in `init` phase, for example)
- Nginx worker is exiting
- `delay` is not a positive number, greater than zero
- Async lawn is full (too many jobs waiting to be queued)
- Async bucket is full (too many jobs in a bucket (`delay`) waiting to be queued)


#### local ok, err = async:at(delay, func, ...)

Queues a job to be executed after a specified `delay`. This is similar to
[`ngx.timer.at`](https://github.com/openresty/lua-nginx-module#ngxtimerat), but instead
of executing jobs with a timer construct we use a thread pool of light threads to execute it.

Possible errors:
- Async is not yet started (this might change, if we want to enable queuing in `init` phase, for example)
- Nginx worker is exiting
- `delay` is not a positive number, or zero
- Async lawn is full (too many jobs waiting to be queued)
- Async bucket is full (too many jobs in a bucket (`delay`) waiting to be queued)


#### local data, size = async:data(from, to)

Returns raw data of executed jobs start and end times for a period of `from` to `to`.

```lua
{
  { <queue-time>, <start-time>, <end-time> },
  { <queue-time>, <start-time>, <end-time> },
  ...
  { <queue-time>, <start-time>, <end-time> },
}
```


#### local stats = async:stats(opts)

Returns statistics of the executed jobs. You can enable the `latency` and `runtime`
statistics by passing the options:

```lua
local stats = async:stats({
  all = true,
  hour = true,
  minute = true,
})
```

Example output:

```lua
{
  done = 20,
  pending = 0,
  running = 0,
  errored = 0,
  refused = 0,
  latency = {
    all = {
      size   = 20,
      mean   = 1,
      median = 2,
      p95    = 12,
      p99    = 20,
      p999   = 150,
      max    = 200,
      min    = 0,
    },
    hour = {
      size   = 10,
      mean   = 1,
      median = 2,
      p95    = 12,
      p99    = 20,
      p999   = 150,
      max    = 200,
      min    = 0,
    },
    minute = {
      size   = 1,
      mean   = 1,
      median = 2,
      p95    = 12,
      p99    = 20,
      p999   = 150,
      max    = 200,
      min    = 0,
    },
  },
  runtime = {
    all = {
      size   = 20,
      mean   = 1,
      median = 2,
      p95    = 12,
      p99    = 20,
      p999   = 150,
      max    = 200,
      min    = 0,
    },
    hour = {
      size   = 10,
      mean   = 1,
      median = 2,
      p95    = 12,
      p99    = 20,
      p999   = 150,
      max    = 200,
      min    = 0,
    },
    minute = {
      size   = 1,
      mean   = 1,
      median = 2,
      p95    = 12,
      p99    = 20,
      p999   = 150,
      max    = 200,
      min    = 0,
    },
  }
}
```


#### local ok, err = async:patch()

Monkey patches `ngx.timer.*` APIs with this instance of `resty.async`. Because `ngx.timer.*`
is essentially a global value, you can only call this on a single instance. If you try to call
it twice on two different instances, it will give you an error on the last call.

The original Nginx `ngx.timer.*` APIs are still available at instance: `async.ngx.timer.*`.


#### local ok, err = async:unpatch()

Removes monkey patches, and returns the `ngx.timer.*` API to its original state. If the
`ngx.timer.*` API was patched by some other instance using `async:patch()`, the unpatching
will fail, and return error instead. Also if the `async:patch()` was not called by this
instance an error is returned.


## Design

`lua-resty-async` is a Nginx worker based job scheduler library. The library starts three
(out of which one is optional) Nginx timers using `ngx.timer.at` function when calling
the `async:start()`:

1. A thread pool timer that spawns, monitors and respawns the light threads.
2. A queue timer that queues new jobs for the light threads.
3. An optional log timer that periodically logs statistics to Nginx error log

The light threads, managed by the thread pool timer, receive work (or the jobs) from the queue timer
or directly through `async:run(func, ...)` using a semaphore (`ngx.semaphore`) that the threads are
waiting on.

The queue works in buckets. Each interval (or delay) gets its own bucket. Each bucket has a fixed
size  and works as a circular wheel where head points to the latest added job and the tail points to
the previously  executed job. The queue timer loops through buckets, and on each bucket queues the
expired jobs for a light thread by releasing a semaphore. The queuing process is fast and doesn't
yield.

The API is compatible with `ngx.timer.at` and `ngx.timer.every`, and takes the same parameters.

(The above three (or two) timers could be reduced to just a one master timer, that spawns three
light threads, but currently I don't see much benefit on it, and it adds another layer of indirection,
so just to point it out, if someone is wondering.)


## TODO

- Optionally allow individual jobs to be named OR grouped, and allow named jobs or groups to be 
  stopped / removed:
  - `async:add(name, func, ...)`
  - `async:run(name, [...])`
  - `async:at(0, name, [...])`
  - `async:every(0, name, [...])`
  - `async:remove(name)`
  - `async:pause(name)`
- Add `async:info()` function to give information about buckets etc., and their fill rate.
- Add test suite and CI


## License

```license
Copyright (c) 2022, Aapo Talvensaari
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```