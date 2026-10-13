<?php

declare(strict_types=1);

namespace FiberInterop\Revolt;

use Revolt\EventLoop\CallbackType;
use Revolt\EventLoop\Driver;
use Revolt\EventLoop\Suspension;

/**
 * Decorates a Revolt driver so that fiber_interop\scope\activate() runs whenever the loop hands control to
 * user code: before every callback (which also runs in the Scope current at registration), right after a Suspension returns in the resumed fiber, and when run()
 * returns to {main}. Covers code that uses Revolt or Amp directly. activate() is lazy (one comparison).
 *
 * @internal
 */
final class ActivatingDriver implements ScopeActivatingDriver
{
    public function __construct(public readonly Driver $inner)
    {
    }

    public function getInner(): Driver { return $this->inner; }

    public static function activate(): void
    {
        if (\function_exists('fiber_interop\scope\activate')) {
            \fiber_interop\scope\activate();
        }
    }

    private static function wrap(\Closure $cb): \Closure
    {
        // A callback runs in the Scope that was current when it was registered (Scope::run() activates it).
        $scope = \Scope::getCurrent();
        return static fn(...$args) => $scope->run($cb, ...$args);
    }

    public function run(): void
    {
        try {
            $this->inner->run();
        } finally {
            self::activate();
        }
    }

    public function stop(): void { $this->inner->stop(); }

    public function getSuspension(): Suspension
    {
        $inner = $this->inner->getSuspension();
        return new class ($inner) implements Suspension {
            public function __construct(private readonly Suspension $inner) {}
            public function resume(mixed $value = null): void { $this->inner->resume($value); }
            public function throw(\Throwable $throwable): void { $this->inner->throw($throwable); }
            public function suspend(): mixed
            {
                try {
                    return $this->inner->suspend();
                } finally {
                    ActivatingDriver::activate();
                }
            }
        };
    }

    public function isRunning(): bool { return $this->inner->isRunning(); }
    public function queue(\Closure $closure, mixed ...$args): void { $this->inner->queue(self::wrap($closure), ...$args); }
    public function defer(\Closure $closure): string { return $this->inner->defer(self::wrap($closure)); }
    public function delay(float $delay, \Closure $closure): string { return $this->inner->delay($delay, self::wrap($closure)); }
    public function repeat(float $interval, \Closure $closure): string { return $this->inner->repeat($interval, self::wrap($closure)); }
    public function onReadable(mixed $stream, \Closure $closure): string { return $this->inner->onReadable($stream, self::wrap($closure)); }
    public function onWritable(mixed $stream, \Closure $closure): string { return $this->inner->onWritable($stream, self::wrap($closure)); }
    public function onSignal(int $signal, \Closure $closure): string { return $this->inner->onSignal($signal, self::wrap($closure)); }
    public function enable(string $callbackId): string { return $this->inner->enable($callbackId); }
    public function cancel(string $callbackId): void { $this->inner->cancel($callbackId); }
    public function disable(string $callbackId): string { return $this->inner->disable($callbackId); }
    public function reference(string $callbackId): string { return $this->inner->reference($callbackId); }
    public function unreference(string $callbackId): string { return $this->inner->unreference($callbackId); }
    public function setErrorHandler(?\Closure $errorHandler): void { $this->inner->setErrorHandler($errorHandler); }
    public function getErrorHandler(): ?\Closure { return $this->inner->getErrorHandler(); }
    public function getHandle(): mixed { return $this->inner->getHandle(); }
    public function getIdentifiers(): array { return $this->inner->getIdentifiers(); }
    public function getType(string $callbackId): CallbackType { return $this->inner->getType($callbackId); }
    public function isEnabled(string $callbackId): bool { return $this->inner->isEnabled($callbackId); }
    public function isReferenced(string $callbackId): bool { return $this->inner->isReferenced($callbackId); }
    public function __debugInfo(): array { return $this->inner->__debugInfo(); }
}
