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
    }

    public function go(\Closure $f): object
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
            if ($e->getPrevious() instanceof \Amp\TimeoutException) {
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
        $suspended = false;
        $result = null;
        $cancel = null;
        $timer = null;

        $settle = static function (bool $ok, mixed $value) use (&$done, &$suspended, &$result, &$cancel, &$timer, $suspension): void {
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
            } else {
                EventLoop::stop(); // let the in-place driver loop return
            }
        };
        $inScope = static fn(\Closure $cb): \Closure => static fn(...$a) => $scope->run($cb, ...$a);
        $scoped = static fn(bool $ok, mixed $value) => $scope->run($settle, $ok, $value);

        $cancel = $subscribe($scoped);
        if (!$done && $timeout !== \PHP_FLOAT_MAX) {
            $timer = EventLoop::delay(\max(0.0, $timeout), $inScope(static fn() => $settle(
                false,
                new Timeout(\sprintf('Timed out after %.3fs', $timeout)),
            )));
        }

        if (!$done) {
            if ($suspension !== null) {
                $suspended = true;
                $suspension->suspend();
            } else {
                $this->driveInPlace($done);
            }
        }

        [$ok, $value] = $result;
        if (!$ok) {
            throw $value;
        }
        return $value;
    }

    /**
     * Run the loop from inside a fiber nobody owns. EventLoop::run() refuses to run inside a fiber, so
     * resume the driver's loop fiber through the same closure DriverSuspension uses for {main}.
     */
    private function driveInPlace(bool &$done): void
    {
        $driver = EventLoop::getDriver();
        $run = (new \ReflectionProperty(DriverSuspension::class, 'run'))->getValue(EventLoop::getSuspension());
        while (!$done) {
            try {
                $interrupt = $run();
            } catch (\FiberError $e) {
                // The driver's loop fiber is the one currently executing (we were resumed by a Revolt callback).
                throw new \LogicException('fiber-interop/revolt: cannot wait in place from a fiber nobody owns while the Revolt loop is running it', 0, $e);
            }
            if ($interrupt !== null) {
                $interrupt(); // an uncaught callback exception surfaces here, as with EventLoop::run()
            }
            if (!$done && $driver->getIdentifiers() === []) {
                throw new \LogicException('Deadlock: waiting with nothing scheduled');
            }
        }
    }
}
