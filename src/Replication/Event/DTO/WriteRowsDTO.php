<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event\DTO;

use BinlogRelay\Replication\Definitions\ConstEventsNames;

class WriteRowsDTO extends RowsDTO
{
    protected ConstEventsNames $type = ConstEventsNames::WRITE;

    public function getType(): string
    {
        return $this->type->value;
    }
}
