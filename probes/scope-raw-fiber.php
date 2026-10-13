<?php
// Portable: a plain Fiber inside a scope sees the creator's $_GET before and after a bare Fiber::suspend().
require __DIR__ . '/../vendor/autoload.php';

use function async\run;

$fail = [];
run(function () use (&$fail) {
    $scope = new \Scope();
    $scope->run(function () use (&$fail) {
        $_GET = ['v' => 'creator'];
        $fiber = new \Fiber(function () use (&$fail) {
            $reads = [$_GET['v'] ?? null];
            \Fiber::suspend();
            $reads[] = $_GET['v'] ?? null;
            foreach ($reads as $i => $r) {
                $r === 'creator' || $fail[] = "raw fiber read $i saw " . json_encode($r);
            }
        });
        $fiber->start();
        $fiber->resume();
    });
});
echo $fail ? implode("\n", $fail) . "\n" : "ok\n";
exit($fail ? 1 : 0);
