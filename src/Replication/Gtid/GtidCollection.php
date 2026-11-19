<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Gtid;

use BinlogRelay\Replication\Support\Collection;
use BinlogRelay\Replication\BinaryDataReader\BinaryDataReader;

/**
 * @extends Collection<int, Gtid>
 */
class GtidCollection extends Collection
{
    public static function makeCollectionFromString(string $gtids): self
    {
        $collection = new self();
        foreach (array_filter(explode(',', $gtids)) as $gtid) {
            $collection->add(new Gtid($gtid));
        }

        return $collection;
    }

    public function getEncodedLength(): int
    {
        $l = 8;
        foreach ($this->toArray() as $gtid) {
            $l += $gtid->getEncodedLength();
        }

        return $l;
    }

    public function getEncoded(): string
    {
        $s = BinaryDataReader::pack64bit($this->count());
        foreach ($this->toArray() as $gtid) {
            $s .= $gtid->getEncoded();
        }

        return $s;
    }
}
