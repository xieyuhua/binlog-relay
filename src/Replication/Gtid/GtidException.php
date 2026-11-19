<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Gtid;

use BinlogRelay\Replication\Exception\MySQLReplicationException;

class GtidException extends MySQLReplicationException
{
    public const INCORRECT_GTID_MESSAGE = 'Incorrect gtid';
    public const INCORRECT_GTID_CODE = 200;
}
