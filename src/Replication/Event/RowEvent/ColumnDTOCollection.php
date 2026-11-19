<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event\RowEvent;

use BinlogRelay\Replication\Support\Collection;
use JsonSerializable;

/**
 * @extends Collection<int, ColumnDTO>
 */
class ColumnDTOCollection extends Collection implements JsonSerializable
{
    public function jsonSerialize(): array
    {
        return $this->toArray();
    }
}
