<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Support;

use ArrayAccess;
use Countable;
use IteratorAggregate;
use JsonSerializable;
use Traversable;

/**
 * 轻量集合（替代 doctrine/collections 的 ArrayCollection）
 *
 * 解析库只用到 add / get / toArray / isEmpty / count / offsetExists / offsetSet，
 * 这里实现这些 + ArrayAccess / Countable / IteratorAggregate，行为与原类一致。
 *
 * @implements ArrayAccess<int|string, mixed>
 * @implements IteratorAggregate<int|string, mixed>
 */
class Collection implements ArrayAccess, Countable, IteratorAggregate, JsonSerializable
{
    /** @var array<int|string, mixed> */
    protected array $elements = [];

    /** @param array<int|string, mixed> $elements */
    public function __construct(array $elements = [])
    {
        $this->elements = $elements;
    }

    public function add(mixed $element): void
    {
        $this->elements[] = $element;
    }

    public function get(int|string $key): mixed
    {
        return $this->elements[$key] ?? null;
    }

    public function set(int|string $key, mixed $value): void
    {
        $this->elements[$key] = $value;
    }

    public function first(): mixed
    {
        return $this->elements === [] ? null : reset($this->elements);
    }

    public function isEmpty(): bool
    {
        return $this->elements === [];
    }

    /** @return array<int|string, mixed> */
    public function toArray(): array
    {
        return $this->elements;
    }

    public function count(): int
    {
        return count($this->elements);
    }

    public function getIterator(): Traversable
    {
        return new \ArrayIterator($this->elements);
    }

    public function offsetExists(mixed $offset): bool
    {
        return isset($this->elements[$offset]);
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->elements[$offset] ?? null;
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        if ($offset === null) {
            $this->elements[] = $value;
        } else {
            $this->elements[$offset] = $value;
        }
    }

    public function offsetUnset(mixed $offset): void
    {
        unset($this->elements[$offset]);
    }

    public function jsonSerialize(): array
    {
        return $this->elements;
    }
}
