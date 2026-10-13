# fiber-interop/revolt

Runs the [`fiber-interop/api`](../api) functions (`async\run`, `go`, `service`, `await`, `await_all`,
`await_any`, `sleep`, `readable`, `writable`; every call happens inside `run()`) on the [Revolt](https://revolt.run) event loop, the loop under AMPHP v3
and usable under ReactPHP.

## Install

    composer require fiber-interop/revolt

Requiring the package registers the adapter (`async\register(...)` from Composer `files` autoload).
Nothing else to configure, unless several adapters are installed, then call `async\setup()`.

## Mapping

| fiber-interop      | Revolt                                                              |
|--------------------|---------------------------------------------------------------------|
| `go()`             | new `Fiber` started from `EventLoop::queue()`; returns a `Task`     |
| `service()`        | like `go()`, but every watcher the service creates is `EventLoop::unreference()`d, so a sleeping or I/O-waiting service never keeps the loop (or the outermost `run()`) alive |
| waiting            | `EventLoop::getSuspension()` -> `suspend()` / `resume()`            |
| `sleep()`          | `EventLoop::delay()`                                                |
| `readable/writable`| `EventLoop::onReadable/onWritable`, cancelled after one firing or on timeout; any number of fibers may wait on one stream |
| timeouts           | `EventLoop::delay()`, cancelled on completion; the awaited task keeps running, a late result is ignored |
| unawaited failure  | rethrown from a queued callback, so it reaches `EventLoop::setErrorHandler()` (uncaught: the loop dies, like any Revolt error) |

Callbacks and task bodies run in the `\Scope` that was current when they were queued.

## AMPHP and ReactPHP users

`await()` natively accepts, when the classes exist:

* `Amp\Future`: uses `$future->await(new TimeoutCancellation($t))`; only that adapter-owned
  `TimeoutCancellation` becomes `async\TimeoutException`. Any other `CancelledException` (including one from a
  `TimeoutCancellation` the awaited future used itself) becomes `FiberInterop\Revolt\Cancelled`
  (implements `async\CancelledException`).
* `React\Promise\PromiseInterface` and any object with `then()` (Guzzle, ...): rejection with a non-Throwable
  becomes `async\RejectedException`.
* the `Task` returned by `go()` also has `then()`, so other adapters can await it.

So `Amp\async()` fibers can call `async\sleep()`, `async\await()` etc. directly, and AMPHP v3 code and
`fiber-interop` code interleave on the same loop. For ReactPHP, run React on Revolt (`react/event-loop`
with the Revolt-backed loop) so both share one driver.

## Limitations: the in-place rule

A wait is done through a Revolt `Suspension` when the caller is the outermost `run()`'s `{main}`, a fiber created by `go()`, or a
Revolt callback fiber (`EventLoop::queue()`, `Amp\async()`, `defer`, watcher callbacks).

Any other fiber (one created by user code that nobody's loop owns) must not be suspended. Revolt's
`Suspension::suspend()` would call `Fiber::suspend()` on it, and `EventLoop::run()` throws inside any fiber.
The adapter therefore drives the loop in place: it resumes the driver's loop fiber directly (through the
closure `DriverSuspension` itself uses for {main}, read by reflection from a private property, so this is tied to
Revolt 1.x internals) and returns when the awaited thing completes, without suspending the foreign fiber.

* If the Revolt loop is currently executing (the foreign fiber was started from inside a task or callback, so
  Revolt's loop fiber is an ancestor on the stack and cannot be resumed re-entrantly), the adapter instead runs
  the driver's own `tick()` and `invokeCallbacks()` by hand from inside the foreign fiber, on a fresh callback
  fiber (also via reflection). Other tasks keep running while the foreign fiber waits.
* Nested in-place waits across two foreign fibers are not supported.
* In-place waiting on an `Amp\Future` observes it through `map()/catch()` instead of `Future::await()`.
* Waiting with nothing scheduled in place throws `LogicException('Deadlock ...')`. In {main}, Revolt itself throws.

## Scope state

Per-scope superglobals (`$_GET`, `$_POST`, `$_COOKIE`, `$_FILES`) follow `fiber-interop/scope`: the adapter
calls `fiber_interop\scope\activate()` (lazy, a swap only on a real scope change) whenever the loop hands
control to user code. The call sites live in `FiberInterop\Revolt\ActivatingDriver`, a decorator around the
Revolt driver installed with `EventLoop::setDriver()` when the package is autoloaded (and again, idempotently,
from the `Adapter` constructor; it cannot be installed while the loop is running):

* every callback registered through `defer/delay/repeat/onReadable/onWritable/onSignal/queue` is wrapped so it
  runs in the `Scope` that was current at registration (`Scope::run()` activates on entry and exit);
* the `Suspension` returned by `getSuspension()` calls `activate()` right after `suspend()` returns, i.e. first
  thing in the resumed fiber;
* `run()` calls `activate()` after the inner `run()` returns, so {main} sees its own scope again;
* the in-place wait (foreign fibers) calls `activate()` when it returns inside the foreign fiber.

Because this sits in the driver, code that uses Revolt or Amp directly is covered, not only code going through
`async\`. The in-place driving (`driveNested`) still uses reflection on `AbstractDriver`; the decorator cannot
replace it because the work is calling the driver's private `tick()`/`invokeCallbacks()`, so it stays as is.

Probes (`php probes/<name>.php`, each prints `ok`):

* `scope-api.php` passes: two scopes interleaved with `async\sleep()` keep their own `$_GET`; root is restored.
* `scope-loop.php` passes: the same scenario with `EventLoop::delay` + `Suspension`, and with `Amp\async` +
  `Amp\delay`, without any `async\` call. It only passes because of the decorator.
* `scope-raw-fiber.php` passes: a plain `new Fiber` inside a scope reads the creator's `$_GET` before and after
  a bare `Fiber::suspend()`. This is the polyfill's "most recently current" fallback, not something the adapter does.

Callbacks registered *before* the package was loaded (or on a driver replaced later with `EventLoop::setDriver()`)
are not wrapped.

## Tests

    composer install
    vendor/bin/phpunit
    vendor/bin/fiber-interop-conformance tests/bootstrap.php
