<?php

declare(strict_types=1);

namespace FiberInterop\Revolt\Tests;

use Amp\Future;
use async\RejectedException;
use async\TimeoutException;
use PHPUnit\Framework\TestCase;
use React\Promise\Deferred;
use React\Promise\Promise;
use Revolt\EventLoop;

use function async\{await, await_all, go, readable, run, sleep};

final class AdapterTest extends TestCase
{
    public function testAmpFutureAwait(): void
    {
        $this->assertSame(42, run(fn() => await(\Amp\async(function () { \Amp\delay(0.01); return 42; }))));
        $this->assertSame(1, await(Future::complete(1)));
    }

    public function testAmpFutureError(): void
    {
        $this->expectExceptionMessage('boom');
        await(Future::error(new \RuntimeException('boom')));
    }

    public function testAmpFutureTimeoutTranslated(): void
    {
        $future = \Amp\async(function () { \Amp\delay(0.5); return 1; });
        $start = microtime(true);
        try {
            await($future, 0.02);
            $this->fail('expected timeout');
        } catch (TimeoutException $e) {
            $this->assertLessThan(0.3, microtime(true) - $start);
        }
        $this->assertSame(1, await($future)); // the task kept running
    }

    public function testAmpFutureInsideGo(): void
    {
        $t = go(fn() => await(\Amp\async(fn() => \Amp\delay(0.01) ?? 'amp')));
        $this->assertSame('amp', await($t));
    }

    public function testReactPromiseAwait(): void
    {
        $d = new Deferred();
        EventLoop::delay(0.01, fn() => $d->resolve('react'));
        $this->assertSame('react', await($d->promise()));
        $this->assertSame('now', await(\React\Promise\resolve('now')));
    }

    public function testNonThrowableRejection(): void
    {
        // React v3 itself refuses non-Throwable reasons; legacy promise libraries still produce them.
        $legacy = new class { public function then(callable $ok, callable $err): void { $err('plain string'); } };
        $this->expectException(RejectedException::class);
        await($legacy);
    }

    public function testReactPromiseTimeout(): void
    {
        $this->expectException(TimeoutException::class);
        await((new Deferred())->promise(), 0.02);
    }

    public function testSettledThenable(): void
    {
        $thenable = new class {
            public function then(callable $ok, callable $err): void { $ok('sync'); }
        };
        $this->assertSame('sync', await($thenable));
    }

    public function testTaskThenForOtherLoops(): void
    {
        $task = go(fn() => 'x');
        $got = null;
        $task->then(function ($v) use (&$got) { $got = $v; });
        sleep(0.01);
        $this->assertSame('x', $got);
    }

    public function testTwoReadableWaitersOnOneStream(): void
    {
        [$r, $w] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        $order = [];
        $a = go(function () use ($r, &$order) { readable($r); $order[] = 'a'; });
        $b = go(function () use ($r, &$order) { readable($r); $order[] = 'b'; });
        $c = go(function () use ($w, &$order) { sleep(0.01); fwrite($w, 'x'); $order[] = 'w'; });
        await_all([$a, $b, $c]);
        sort($order);
        $this->assertSame(['a', 'b', 'w'], $order);
    }

    public function testReadableTimeoutCancelsWatcher(): void
    {
        [$r, $keep] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        $before = count(EventLoop::getIdentifiers());
        try {
            readable($r, 0.01);
            $this->fail('expected timeout');
        } catch (TimeoutException) {
        }
        $this->assertCount($before, EventLoop::getIdentifiers());
        $this->assertNotNull($keep);
    }

    public function testWaitingInsideAmpAsyncFiberSuspends(): void
    {
        // Amp\async() fibers are Revolt callback fibers: they must suspend through Revolt, not run in place.
        $f = \Amp\async(function () {
            sleep(0.01);
            return await(go(fn() => 'from amp fiber'));
        });
        $this->assertSame('from amp fiber', $f->await());
    }

    public function testForeignFiberAwaitsAmpFutureInPlace(): void
    {
        $fiber = new \Fiber(fn() => await(\Amp\async(fn() => \Amp\delay(0.01) ?? 'in place')));
        $fiber->start();
        $this->assertTrue($fiber->isTerminated());
        $this->assertSame('in place', $fiber->getReturn());
    }

    public function testUnawaitedFailureReachesLoopErrorHandler(): void
    {
        $seen = [];
        EventLoop::setErrorHandler(function (\Throwable $e) use (&$seen) { $seen[] = $e->getMessage(); });
        try {
            go(function () { throw new \RuntimeException('nobody awaits'); });
            $awaited = go(function () { throw new \RuntimeException('awaited'); });
            try { await($awaited); } catch (\RuntimeException) {}
            sleep(0.01);
        } finally {
            EventLoop::setErrorHandler(null);
        }
        $this->assertSame(['nobody awaits'], $seen);
    }
}
