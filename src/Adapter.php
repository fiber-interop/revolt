<?php

declare(strict_types=1);

namespace FiberInterop\Revolt;

use async\RejectedException;
use async\Timeout;
use Revolt\EventLoop;
use Revolt\EventLoop\Internal\DriverSuspension;

/**
 * fiber-interop adapter on the Revolt event loop.
 *
 * Waiting strategy (see README, "In-place rule"):
 *  - {main}, a fiber created by go(), or a fiber running inside a Revolt callback: Revolt Suspension.
 *  - any other fiber (one nobody's loop owns): the loop is driven in place, from inside that fiber.
 */
final class Adapter implements \async\Adapter
{
    /** @var \WeakMap<\Fiber, true> fibers created by go() */
    private \WeakMap $own;

    public function __construct()
    {
        $this->own = new \WeakMap();
        $this->services = new \WeakMap();
        self::decorateDriver();
    }

    /**
     * Wrap the Revolt driver in ActivatingDriver (once), so code using Revolt or Amp directly is covered too.
     * Called when the package is autoloaded and again from the constructor; not possible while the loop runs.
     */
    public static function decorateDriver(): void
    {
        $driver = EventLoop::getDriver();
        $decorated = false;
        foreach (\class_implements($driver) as $interface) {
            if (\str_ends_with($interface, '\\ScopeActivatingDriver')) {
                $decorated = true;
                break;
            }
        }
        if (!$decorated && !$driver->isRunning()) {
            EventLoop::setDriver(new ActivatingDriver($driver));
        }
    }

    /** @var \WeakMap<\Fiber, true> fibers created by service(): their watchers are unreferenced */
    private \WeakMap $services;

    public function go(\Closure $f): object
    {
        return $this->spawn($f, false);
    }

    /**
     * A service is a task whose watchers are unreferenced (EventLoop::unreference), so a sleeping
     * or I/O-waiting service never keeps the loop, and thus the outermost run(), alive.
     */
    public function service(\Closure $f): object
    {
        return $this->spawn($f, true);
    }

    private function spawn(\Closure $f, bool $service): Task
    {
        $task = new Task();
        $scope = \Scope::getCurrent();
        $fiber = new \Fiber(function () use ($f, $task, $scope): void {
            try {
                $task->settle(true, $scope->run($f));
            } catch (\Throwable $e) {
                $task->settle(false, $e);
                // Give an awaiter a chance to attach; if nobody did, the loop's error handler sees it.
                EventLoop::queue(static function () use ($task, $e): void {
                    if (!$task->handled) {
                        throw $e;
                    }
                });
            }
        });
        $this->own[$fiber] = true;
        $service && $this->services[$fiber] = true;
        EventLoop::queue(static fn() => $scope->run(static fn() => $fiber->start()));
        return $task;
    }

    public function canAwait(object $o): bool
    {
        return $o instanceof Task
            || (\class_exists(\Amp\Future::class) && $o instanceof \Amp\Future)
            || (\interface_exists(\React\Promise\PromiseInterface::class) && $o instanceof \React\Promise\PromiseInterface)
            || \method_exists($o, 'then');
    }

    public function await(object $o, float $timeout): mixed
    {
        if ($o instanceof Task) {
            $o->handled = true;
            return $this->wait(static function (\Closure $settle) use ($o) {
                $o->onSettle($settle);
            }, $timeout);
        }

        if (\class_exists(\Amp\Future::class) && $o instanceof \Amp\Future && $this->canSuspend()) {
            return $this->awaitAmp($o, $timeout);
        }

        if (\class_exists(\Amp\Future::class) && $o instanceof \Amp\Future) {
            // In-place: Future::await() would suspend the foreign fiber, so observe it from a callback instead.
            return $this->wait(static function (\Closure $settle) use ($o) {
                $o->map(static fn($v) => $settle(true, $v))
                    ->catch(static fn(\Throwable $e) => $settle(false, $e))
                    ->ignore();
            }, $timeout);
        }

        return $this->wait(static function (\Closure $settle) use ($o) {
            $o->then(
                static fn($v) => $settle(true, $v),
                static fn($r) => $settle(false, $r instanceof \Throwable ? $r : new RejectedException($r)),
            );
        }, $timeout);
    }

    private function awaitAmp(\Amp\Future $future, float $timeout): mixed
    {
        $cancellation = $timeout === \PHP_FLOAT_MAX ? null : new \Amp\TimeoutCancellation($timeout);
        try {
            return $future->await($cancellation);
        } catch (\Amp\CancelledException $e) {
            // Only our own TimeoutCancellation is a Timeout; any other CancelledException (even one caused by
            // a TimeoutCancellation inside the awaited future) is a Cancelled.
            if ($cancellation !== null && $cancellation->isRequested() && $e->getPrevious() instanceof \Amp\TimeoutException) {
                throw new Timeout(\sprintf('Timed out after %.3fs', $timeout), 0, $e);
            }
            throw new Cancelled($e->getMessage(), 0, $e);
        }
    }

    public function sleep(float $seconds): void
    {
        $this->wait(function (\Closure $settle) use ($seconds) {
            $id = EventLoop::delay(\max(0.0, $seconds), static fn() => $settle(true, null));
            return static fn() => EventLoop::cancel($id);
        }, \PHP_FLOAT_MAX);
    }

    public function readable(mixed $resource, float $timeout): void
    {
        $this->waitStream('onReadable', $resource, $timeout);
    }

    public function writable(mixed $resource, float $timeout): void
    {
        $this->waitStream('onWritable', $resource, $timeout);
    }

    private function waitStream(string $method, mixed $resource, float $timeout): void
    {
        $this->wait(static function (\Closure $settle) use ($method, $resource) {
            $id = EventLoop::$method($resource, static fn() => $settle(true, null));
            return static fn() => EventLoop::cancel($id);
        }, $timeout);
    }

    /** May the current context suspend through Revolt's Suspension? */
    private function canSuspend(): bool
    {
        $fiber = \Fiber::getCurrent();
        if ($fiber === null || isset($this->own[$fiber])) {
            return true;
        }
        // debug_backtrace() spans fibers. Walking down from here, the first of {Revolt driver frame, Fiber::start/resume}
        // tells who entered the current fiber: the driver (a Revolt callback fiber, e.g. Amp\async()) or user code.
        foreach (\debug_backtrace(\DEBUG_BACKTRACE_IGNORE_ARGS) as $frame) {
            $class = $frame['class'] ?? '';
            if ($class === \Fiber::class) {
                return false;
            }
            if (\str_starts_with($class, 'Revolt\\EventLoop\\Internal\\')) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param \Closure(\Closure(bool, mixed): void): (\Closure|null) $subscribe registers interest, returns an optional canceller
     */
    private function wait(\Closure $subscribe, float $timeout): mixed
    {
        $suspend = $this->canSuspend();
        $scope = \Scope::getCurrent();
        $suspension = $suspend ? EventLoop::getSuspension() : null;
        $done = false;
        $nested = false;
        $suspended = false;
        $result = null;
        $cancel = null;
        $timer = null;

        $settle = static function (bool $ok, mixed $value) use (&$done, &$nested, &$suspended, &$result, &$cancel, &$timer, $suspension): void {
            if ($done) {
                return;
            }
            $done = true;
            $result = [$ok, $value];
            if ($cancel !== null) {
                $cancel();
            }
            if ($timer !== null) {
                EventLoop::cancel($timer);
            }
            if ($suspension !== null) {
                $suspended && $suspension->resume();
            } elseif (!$nested) {
                EventLoop::stop(); // let the in-place driver loop return
            }
        };
        $inScope = static fn(\Closure $cb): \Closure => static fn(...$a) => $scope->run($cb, ...$a);
        $scoped = static fn(bool $ok, mixed $value) => $scope->run($settle, $ok, $value);

        $service = ($fiber = \Fiber::getCurrent()) !== null && isset($this->services[$fiber]);
        $before = $service ? EventLoop::getIdentifiers() : [];
        $cancel = $subscribe($scoped);
        if (!$done && $timeout !== \PHP_FLOAT_MAX) {
            $timer = EventLoop::delay(\max(0.0, $timeout), $inScope(static fn() => $settle(
                false,
                new Timeout(\sprintf('Timed out after %.3fs', $timeout)),
            )));
        }
        if ($service) {
            foreach (\array_diff(EventLoop::getIdentifiers(), $before) as $id) {
                EventLoop::unreference($id);
            }
        }

        if (!$done) {
            if ($suspension !== null) {
                $suspended = true;
                $suspension->suspend();
            } else {
                $this->driveInPlace($done, $nested);
            }
        }

        if (!$suspend) {
            ActivatingDriver::activate(); // in-place wait returned inside the foreign fiber
        }

        [$ok, $value] = $result;
        if (!$ok) {
            throw $value;
        }
        return $value;
    }

    /**
     * Run the loop from inside a fiber nobody owns.
     *
     * EventLoop::run() refuses to run inside a fiber, so there are two cases:
     *  - the driver's loop fiber is not running (we are in {main}'s call stack, e.g. a foreign fiber started
     *    from {main}): resume it through the closure DriverSuspension uses for {main}.
     *  - the loop fiber is running (the foreign fiber was started from a task, so Revolt is an ancestor on
     *    the stack and cannot be resumed): run the driver's own tick/invokeCallbacks by hand from here, on
     *    a fresh callback fiber (the current one is an ancestor on the stack too).
     *    See driveNested().
     */
    private function driveInPlace(bool &$done, bool &$nested): void
    {
        $driver = EventLoop::getDriver();
        $driver instanceof ActivatingDriver && $driver = $driver->inner;
        $ref = new \ReflectionClass(\Revolt\EventLoop\Internal\AbstractDriver::class);
        $loopFiber = $ref->getProperty('fiber')->getValue($driver);
        if ($loopFiber->isRunning()) {
            $nested = true;
            $this->driveNested($driver, $ref, $done);
            return;
        }
        $run = (new \ReflectionProperty(DriverSuspension::class, 'run'))->getValue($driver->getSuspension());
        while (!$done) {
            $interrupt = $run();
            if ($interrupt !== null) {
                $interrupt(); // an uncaught callback exception surfaces here, as with EventLoop::run()
            }
            if (!$done && $driver->getIdentifiers() === []) {
                throw new \LogicException('Deadlock: waiting with nothing scheduled');
            }
        }
    }

    private function driveNested(object $driver, \ReflectionClass $ref, bool &$done): void
    {
        $prop = static fn(string $n) => $ref->getProperty($n)->getValue($driver);
        $call = static fn(string $n, mixed ...$a) => $ref->getMethod($n)->invoke($driver, ...$a);
        $tick = new \ReflectionMethod($driver, 'tick');
        $call('createCallbackFiber');
        while (!$done) {
            $call('invokeCallbacks');
            if ($done) {
                return;
            }
            if ($driver->getIdentifiers() === []) {
                throw new \LogicException('Deadlock: waiting with nothing scheduled');
            }
            $idle = $prop('microtaskQueue')->isEmpty() && $prop('callbackQueue')->isEmpty();
            $tick->invoke($driver, $idle);
        }
    }
}
