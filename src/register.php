<?php

declare(strict_types=1);

\fiber\register(static fn() => new \FiberInterop\Revolt\Adapter(), 'fiber-interop/revolt');
\FiberInterop\Revolt\Adapter::decorateDriver();
