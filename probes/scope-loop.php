<?php
// Runtime-specific: the same scenario with Revolt primitives only (and Amp\async + Amp\delay as variant 2).
require __DIR__ . '/../vendor/autoload.php';

use Revolt\EventLoop;

$fail = [];
$check = function (string $name, int $i) use (&$fail) {
    if (($_GET['v'] ?? null) !== $name) {
        $fail[] = "scope $name step $i saw " . json_encode($_GET);
    }
};

// Variant 1: EventLoop::delay callbacks + Suspension.
foreach (['a', 'b'] as $name) {
    $scope = new \Scope();
    EventLoop::queue(fn() => $scope->run(function () use ($name, $check) {
        $_GET = ['v' => $name];
        for ($i = 0; $i < 2; $i++) {
            $s = EventLoop::getSuspension();
            EventLoop::delay(0.01, fn() => $s->resume());
            $s->suspend();
            $check($name, $i);
        }
        // a plain delay callback queued from inside the scope runs in that scope
        $s = EventLoop::getSuspension();
        EventLoop::delay(0.01, function () use ($name, $check, $s) { $check($name, 9); $s->resume(); });
        $s->suspend();
    }));
}
EventLoop::run();
if ($_GET !== []) {
    $fail[] = 'variant 1 root $_GET after run: ' . json_encode($_GET);
}

// Variant 2: Amp\async + Amp\delay.
if (function_exists('Amp\async')) {
    $futures = [];
    foreach (['c', 'd'] as $name) {
        $scope = new \Scope();
        $futures[] = $scope->run(fn() => \Amp\async(function () use ($name, $check) {
            $_GET = ['v' => $name];
            for ($i = 0; $i < 2; $i++) {
                \Amp\delay(0.01);
                $check($name, $i);
            }
        }));
    }
    \Amp\Future\await($futures);
    if ($_GET !== []) {
        $fail[] = 'variant 2 root $_GET after await: ' . json_encode($_GET);
    }
}
echo $fail ? implode("\n", $fail) . "\n" : "ok\n";
exit($fail ? 1 : 0);
