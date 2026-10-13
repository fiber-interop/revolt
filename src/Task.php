<?php

declare(strict_types=1);

namespace FiberInterop\Revolt;

/**
 * The opaque awaitable returned by go(). Exposes then() so other adapters (and any
 * promise-style code) can await it across loops.
 */
final class Task
{
    public bool $handled = false;
    /** @var array{bool, mixed}|null [ok, value-or-throwable] */
    private ?array $result = null;
    /** @var list<\Closure(bool, mixed): void> */
    private array $listeners = [];

    public function isSettled(): bool
    {
        return $this->result !== null;
    }

    /** @param \Closure(bool, mixed): void $cb called once with (ok, value|Throwable), at once if already settled */
    public function onSettle(\Closure $cb): void
    {
        $this->result === null ? $this->listeners[] = $cb : $cb(...$this->result);
    }

    public function settle(bool $ok, mixed $value): void
    {
        $this->result = [$ok, $value];
        $listeners = $this->listeners;
        $this->listeners = [];
        foreach ($listeners as $cb) {
            $cb($ok, $value);
        }
    }

    /** Promise-style access; the callbacks may run synchronously when already settled. */
    public function then(?callable $onFulfilled = null, ?callable $onRejected = null): void
    {
        $this->handled = true;
        $this->onSettle(static function (bool $ok, mixed $value) use ($onFulfilled, $onRejected): void {
            $cb = $ok ? $onFulfilled : $onRejected;
            if ($cb !== null) {
                $cb($value);
            }
        });
    }
}
