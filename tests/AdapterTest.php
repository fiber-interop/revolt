<?php

declare(strict_types=1);

namespace FiberInterop\Revolt\Tests;

use Amp\Future;
use fiber\RejectedException;
use fiber\TimeoutException;
use PHPUnit\Framework\TestCase;
use React\Promise\Deferred;
use React\Promise\Promise;
use Revolt\EventLoop;

use function fiber\{await, await_all, go, readable, run, sleep};

final class AdapterTest extends TestCase
{
    public function testLocalCloseUnblocksWaiter(): void
    {
        run(function () {
            [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $woken = 0;
            $t1 = go(function () use ($a, &$woken) { readable($a); $woken++; });
            $t2 = go(function () use ($a, &$woken) { readable($a); $woken++; });
            sleep(0.02);
            $start = microtime(true);
            fclose($a);
            await($t1);
            await($t2);
            $this->assertLessThan(0.1, microtime(true) - $start);
            $this->assertSame(2, $woken);
        });
    }

    public function testLocalCloseInCallbackRecoversWithoutTimer(): void
    {
        run(function () {
            [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $t = go(function () use ($a) { readable($a); return 'woken'; });
            sleep(0.001);
            // Closed from inside a loop callback; the loop selects again immediately afterwards.
            EventLoop::defer(static function () use ($a) { fclose($a); });
            $this->assertSame('woken', await($t));
        });
    }

    public function testAmpFutureAwait(): void
    {
        run(function () {
            $this->assertSame(42, run(fn() => await(\Amp\async(function () { \Amp\delay(0.01); return 42; }))));
            $this->assertSame(1, await(Future::complete(1)));
        });
    }

    public function testAmpFutureError(): void
    {
        run(function () {
            $this->expectExceptionMessage('boom');
            await(Future::error(new \RuntimeException('boom')));
        });
    }

    public function testAmpFutureTimeoutTranslated(): void
    {
        run(function () {
            $future = \Amp\async(function () { \Amp\delay(0.5); return 1; });
            $start = microtime(true);
            try {
                await($future, 0.02);
                $this->fail('expected timeout');
            } catch (TimeoutException $e) {
                $this->assertLessThan(0.3, microtime(true) - $start);
            }
            $this->assertSame(1, await($future)); // the task kept running
        });
    }

    public function testAmpFutureThatTimedOutItselfIsCancelledNotTimeout(): void
    {
        run(function () {
            $slow = \Amp\async(function () { \Amp\delay(0.5); return 1; });
            $future = \Amp\async(fn() => $slow->await(new \Amp\TimeoutCancellation(0.01)));
            try {
                await($future, 2.0);
                $this->fail('expected cancellation');
            } catch (\fiber\CancelledException $e) {
                $this->assertNotInstanceOf(TimeoutException::class, $e);
                $this->assertInstanceOf(\FiberInterop\Revolt\Cancelled::class, $e);
            }
        });
    }

    public function testAmpFutureInsideGo(): void
    {
        run(function () {
            $t = go(fn() => await(\Amp\async(fn() => \Amp\delay(0.01) ?? 'amp')));
            $this->assertSame('amp', await($t));
        });
    }

    public function testReactPromiseAwait(): void
    {
        run(function () {
            $d = new Deferred();
            EventLoop::delay(0.01, fn() => $d->resolve('react'));
            $this->assertSame('react', await($d->promise()));
            $this->assertSame('now', await(\React\Promise\resolve('now')));
        });
    }

    public function testNonThrowableRejection(): void
    {
        run(function () {
            // React v3 itself refuses non-Throwable reasons; legacy promise libraries still produce them.
            $legacy = new class { public function then(callable $ok, callable $err): void { $err('plain string'); } };
            $this->expectException(RejectedException::class);
            await($legacy);
        });
    }

    public function testReactPromiseTimeout(): void
    {
        run(function () {
            $this->expectException(TimeoutException::class);
            await((new Deferred())->promise(), 0.02);
        });
    }

    public function testSettledThenable(): void
    {
        run(function () {
            $thenable = new class {
                public function then(callable $ok, callable $err): void { $ok('sync'); }
            };
            $this->assertSame('sync', await($thenable));
        });
    }

    public function testTaskThenForOtherLoops(): void
    {
        run(function () {
            $task = go(fn() => 'x');
            $got = null;
            $task->then(function ($v) use (&$got) { $got = $v; });
            sleep(0.01);
            $this->assertSame('x', $got);
        });
    }

    public function testTwoReadableWaitersOnOneStream(): void
    {
        run(function () {
            [$r, $w] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $order = [];
            $a = go(function () use ($r, &$order) { readable($r); $order[] = 'a'; });
            $b = go(function () use ($r, &$order) { readable($r); $order[] = 'b'; });
            $c = go(function () use ($w, &$order) { sleep(0.01); fwrite($w, 'x'); $order[] = 'w'; });
            await_all([$a, $b, $c]);
            sort($order);
            $this->assertSame(['a', 'b', 'w'], $order);
        });
    }

    public function testReadableTimeoutCancelsWatcher(): void
    {
        run(function () {
            [$r, $keep] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $before = count(EventLoop::getIdentifiers());
            try {
                readable($r, 0.01);
                $this->fail('expected timeout');
            } catch (TimeoutException) {
            }
            $this->assertCount($before, EventLoop::getIdentifiers());
            $this->assertNotNull($keep);
        });
    }

    public function testWaitingInsideAmpAsyncFiberSuspends(): void
    {
        run(function () {
            // Amp\async() fibers are Revolt callback fibers: they must suspend through Revolt, not run in place.
            $f = \Amp\async(function () {
                sleep(0.01);
                return await(go(fn() => 'from amp fiber'));
            });
            $this->assertSame('from amp fiber', $f->await());
        });
    }

    public function testForeignFiberAwaitsAmpFutureInPlace(): void
    {
        run(function () {
            $fiber = new \Fiber(fn() => await(\Amp\async(fn() => \Amp\delay(0.01) ?? 'in place')));
            $fiber->start();
            $this->assertTrue($fiber->isTerminated());
            $this->assertSame('in place', $fiber->getReturn());
        });
    }

    public function testFailingServiceReachesLoopErrorHandler(): void
    {
        run(function () {
            $seen = [];
            EventLoop::setErrorHandler(function (\Throwable $e) use (&$seen) { $seen[] = $e->getMessage(); });
            try {
                \fiber\service(function () { throw new \RuntimeException('nobody awaits'); });
                sleep(0.01);
            } finally {
                EventLoop::setErrorHandler(null);
            }
            $this->assertSame(['nobody awaits'], $seen);
        });
    }

    public function testDriverIsDecoratedOnce(): void
    {
        \FiberInterop\Revolt\Adapter::decorateDriver();
        \FiberInterop\Revolt\Adapter::decorateDriver();
        $driver = EventLoop::getDriver();
        $this->assertInstanceOf(\FiberInterop\Revolt\ActivatingDriver::class, $driver);
        $this->assertNotInstanceOf(\FiberInterop\Revolt\ActivatingDriver::class, $driver->inner);
    }

    public function testScopeActivatedOnCallback(): void
    {
        $seen = null;
        run(function () use (&$seen) {
            $scope = new \Scope();
            $scope->run(function () use (&$seen) {
                $_GET = ['v' => 'inner'];
                EventLoop::delay(0.001, function () use (&$seen) { $seen = $_GET; });
            });
            $this->assertSame([], $_GET); // back in the root scope
            sleep(0.02);
        });
        $this->assertSame(['v' => 'inner'], $seen);
    }

    public function testScopeActivatedOnResume(): void
    {
        $seen = [];
        run(function () use (&$seen) {
            $tasks = [];
            foreach (['a', 'b'] as $n) {
                $tasks[] = go(function () use ($n, &$seen) {
                    (new \Scope())->run(function () use ($n, &$seen) {
                        $_GET = ['v' => $n];
                        $s = EventLoop::getSuspension();
                        EventLoop::delay($n === 'a' ? 0.02 : 0.01, fn() => $s->resume());
                        $s->suspend();
                        $seen[$n] = $_GET['v'] ?? null;
                    });
                });
            }
            await_all($tasks);
        });
        $this->assertSame(['b' => 'b', 'a' => 'a'], $seen);
    }

    public function testScopeActivatedOnReturnToMain(): void
    {
        $_GET = ['root' => 1];
        EventLoop::queue(static function () {
            (new \Scope())->run(static function () {
                $_GET = ['v' => 'inner'];
                $s = EventLoop::getSuspension();
                EventLoop::delay(0.001, fn() => $s->resume());
                $s->suspend();
                // leave the scope while still inside the loop: the next thing to run is {main} again
            });
        });
        EventLoop::run();
        $this->assertSame(['root' => 1], $_GET);
        $_GET = [];
    }

    public function testNoSwapWhenScopeUnchanged(): void
    {
        run(function () {
            (new \Scope())->run(function () {
                $_GET = ['m' => 1];
                $s = EventLoop::getSuspension();
                EventLoop::delay(0.001, function () use ($s) {
                    $_GET['m']++; // live value written between activations must not be overwritten by a stale swap
                    $s->resume();
                });
                $s->suspend();
                $this->assertSame(['m' => 2], $_GET);
                EventLoop::queue(fn() => $this->assertSame(['m' => 2], $_GET));
                sleep(0.001);
                $this->assertSame(['m' => 2], $_GET);
            });
        });
    }

    #[\PHPUnit\Framework\Attributes\RunInSeparateProcess]
    public function testForeignMarkerDecoratorIsNotWrappedAgain(): void
    {
        $before = EventLoop::getDriver();
        $foreign = new class () extends \Revolt\EventLoop\Internal\AbstractDriver implements \Foreign\ScopeActivatingDriver {
            protected function activate(array $callbacks): void {}
            protected function dispatch(bool $blocking): void {}
            protected function deactivate(\Revolt\EventLoop\Internal\DriverCallback $callback): void {}
            public function getHandle(): mixed { return null; }
            protected function now(): float { return 0.0; }
        };
        EventLoop::setDriver($foreign);
        try {
            \FiberInterop\Revolt\Adapter::decorateDriver();
            $this->assertSame($foreign, EventLoop::getDriver());
        } finally {
            EventLoop::setDriver($before);
        }
    }

    /** Services are driven while a run() is active: awaiting what only a service resolves must not end the loop. */
    public function testAwaitResolvedOnlyByService(): void
    {
        $t = \microtime(true);
        $result = \fiber\run(function () {
            $timer = new \fiber\Deferred();
            \fiber\service(function () use ($timer) {
                \fiber\sleep(0.02);
                $timer->settle(true, 'slept');
                \fiber\sleep(30); // a sleeping service must not keep run() alive
            });
            [$a, $b] = \stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
            $io = new \fiber\Deferred();
            \fiber\service(function () use ($a, $io) {
                \fiber\readable($a, 5.0);
                $io->settle(true, \fread($a, 10));
            });
            \fiber\service(function () use ($b) {
                \fiber\sleep(0.02);
                \fwrite($b, 'ping');
            });
            return [\fiber\await($timer), \fiber\await($io)];
        });
        $this->assertSame(['slept', 'ping'], $result);
        $this->assertLessThan(5.0, \microtime(true) - $t);
    }

    public function testCancelAmpFutureAwaitThrowsTheReason(): void
    {
        run(function () {
            $future = \Amp\async(function () { \Amp\delay(0.05); return 'late'; });
            foreach ([\PHP_FLOAT_MAX, 5.0] as $timeout) { // with and without our own TimeoutCancellation
                $t = go(fn() => await($future, $timeout));
                sleep(0.01);
                $reason = new \fiber\Cancelled('stop');
                \fiber\cancel($t, $reason);
                try {
                    await($t);
                    $this->fail('await returned');
                } catch (\fiber\CancelledException $e) {
                    $this->assertSame($reason, $e);
                }
            }
            $this->assertSame('late', $future->await()); // the future itself was not cancelled
        });
    }

    public function testCancelledWaitIsNotResumedAgain(): void
    {
        run(function () {
            $d = new \fiber\Deferred();
            $t = go(function () use ($d) {
                try {
                    await($d);
                } catch (\fiber\CancelledException $e) {
                    return 'cancelled';
                }
                return 'resumed';
            });
            sleep(0.01);
            \fiber\cancel($t);
            $d->resolve('late'); // must not resume the task's finished wait (Revolt would raise an Error)
            sleep(0.02);
            $this->assertSame('cancelled', await($t));
        });
    }
}
