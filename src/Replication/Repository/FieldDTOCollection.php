<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Repository;

use BinlogRelay\Replication\Support\Collection;

/**
 * @extends Collection<int, FieldDTO>
 */
class FieldDTOCollection extends Collection
{
    public static function makeFromArray(array $fields): self
    {
        $collection = new self();
        foreach ($fields as $field) {
            $collection->add(FieldDTO::makeFromArray($field));
        }

        return $collection;
    }
}
