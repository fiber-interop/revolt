<?php

declare(strict_types=1);

\async\register(static fn() => new \FiberInterop\Revolt\Adapter(), 'fiber-interop/revolt');
