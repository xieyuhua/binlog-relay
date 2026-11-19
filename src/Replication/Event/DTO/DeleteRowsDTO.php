<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event\DTO;

use BinlogRelay\Replication\Definitions\ConstEventsNames;

class DeleteRowsDTO extends RowsDTO
{
    protected ConstEventsNames $type = ConstEventsNames::DELETE;

    public function getType(): string
    {
        return $this->type->value;
    }
}
