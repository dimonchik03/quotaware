# 0016. Hold a fork until the Python hook's delivery thread is idle

Date: 2026-11-15

## Status

Accepted

## Context

The Python hook delivers observations from a daemon thread and promises to keep
reporting in a forked child (Celery prefork, gunicorn `--preload`): an
`os.register_at_fork` handler gives the child a new queue and a new thread. A fork
copies only the thread that called it, and it copies every lock in the state it had at
that instant. The delivery thread spends its working time inside libraries that take
process-wide locks with the GIL released, OpenSSL above all: httpx creates an SSL
context when the thread builds its client, which it does as soon as the hook exists,
and a TLS exchange takes the same locks. A fork that lands while the thread holds one
hands the child a lock that no thread will ever release. The child's new delivery
thread blocks on it for good, nothing is delivered, and once the queue is full every
observation is dropped.

CI hit this on 2026-11-11: in the child `close()` gave up with the observation still
queued. A reproduction shows the child's thread stuck in `SSL_CTX_new`; forking
straight after creating the hook failed in 6 of 200 runs, forking once the thread was
waiting on its queue in none of 1500. With httpx 0.28, which imports `httpcore` when
the first client is built, the same fork can also hand the child a half-imported
module. Python cannot reset the locks of a C library in the child, and a fork handler
cannot refuse a fork. The working agreement forbids logging and continuing without a
recorded decision.

## Decision

The hook registers a `before` and an `after_in_parent` fork handler next to the
existing `after_in_child` one. Before a fork the handler marks the hook as forking and
waits until the delivery thread is idle. The thread is idle only while it waits on the
queue: building the client, posting and closing the client are work, and the thread
starts no work while a fork is under way. The wait is bounded by `timeout` plus one
second, the bound `close()` already uses for the same thread; when it expires the
handler logs a warning through the `quotaware_httpx` logger and the fork goes ahead.
After the fork the parent's thread carries on. The child gets new locks whether or not
the hook was closed, and a new thread unless it was.

## Alternatives considered

- Build the client on the first observation instead of at once: no wait, and it covers
  a master that creates the hook and forks, which is the case CI hit. A process that
  has just observed a call, or is posting when it forks, still hands the child a held
  lock, so the window moves instead of closing.
- Wait without a bound, as `logging` does for its own lock: a delivery thread that is
  itself stuck, in a name lookup that has no timeout or on a lock some other thread
  left behind in an earlier fork, would hang every later fork of that process.
- Leave the fork alone and replace a delivery thread that does not come up in the
  child: a thread blocked inside OpenSSL cannot be cancelled and its replacement
  blocks on the same lock.
- Build the client on the calling thread, in the constructor and in the child's fork
  handler: a lock another thread of the application held at the fork would then hang
  the child inside `os.fork()` instead of costing it its observations.

## Consequences

`os.fork()`, `os.forkpty()`, `multiprocessing` with the fork start method and
`subprocess` with a `preexec_fn` wait for the delivery under way in every live hook:
milliseconds when quotaware answers, up to `timeout` plus one second per hook when it
does not. `subprocess` without a `preexec_fn` runs no fork handlers and is not
affected. When the wait expires the child may not report, and the warning is the only
sign of it. The gate covers the hook's own thread; a fork while another thread of the
application is inside OpenSSL can still deadlock the child, as it could before.
Anything the delivery thread learns to do later has to happen between `_begin_work`
and `_end_work`, so that waiting on the queue stays the only idle state.
