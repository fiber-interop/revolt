<?php

declare(strict_types=1);

namespace FiberInterop\Revolt;

/** Thrown by await() when an Amp\Future was cancelled for a reason other than our timeout. */
final class Cancelled extends \RuntimeException implements \async\CancelledException
{
}
