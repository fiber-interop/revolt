<?php
// Portable: two scopes interleave via async\sleep(); each must keep its own $_GET.
require __DIR__ . '/../vendor/autoload.php';

use function async\{go, await, run, sleep};

$fail = [];
run(function () use (&$fail) {
    $tasks = [];
    foreach (['a', 'b'] as $name) {
        $tasks[] = go(function () use ($name, &$fail) {
            $scope = new \Scope();
            return $scope->run(function () use ($name, &$fail) {
                $_GET = ['v' => $name];
                for ($i = 0; $i < 2; $i++) {
                    sleep(0.01);
                    if (($_GET['v'] ?? null) !== $name) {
                        $fail[] = "scope $name after sleep $i saw " . json_encode($_GET);
                    }
                }
            });
        });
    }
    foreach ($tasks as $t) {
        await($t);
    }
});
if ($_GET !== []) {
    $fail[] = 'root $_GET after run(): ' . json_encode($_GET);
}
echo $fail ? implode("\n", $fail) . "\n" : "ok\n";
exit($fail ? 1 : 0);
