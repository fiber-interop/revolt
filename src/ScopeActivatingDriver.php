<?php

declare(strict_types=1);

namespace FiberInterop\Revolt;

use Revolt\EventLoop\Driver;

/**
 * Marker for a Revolt driver decorator that already activates fiber-interop scopes. Adapters treat any driver
 * implementing an interface named *\ScopeActivatingDriver as already decorated and do not wrap it again.
 */
interface ScopeActivatingDriver extends Driver
{
    public function getInner(): Driver;
}
