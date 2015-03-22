<?php

namespace Pheasant\Database\DBAL;

use Doctrine\DBAL\Statement;

class ResultIterator implements \SeekableIterator, \Countable
{
    private
        $_stmt,
        $_hydrator,
        $_current = null
    ;

    public function __construct(Statement $stmt)
    {
        $this->_stmt = $stmt;
    }

    public function setHydrator($callback)
    {
        $this->_hydrator = $callback;
    }

    public function valid()
    {
        return false !== $this->_current;
    }

    public function current()
    {
        return $this->_current;
    }

    public function next()
    {
        $next = $this->_stmt->fetch(\PDO::FETCH_ASSOC);
        if ($next && isset($this->_hydrator)) {
            $next = call_user_func($this->_hydrator, $next);
        }

        // promote next to current
        $this->_current = $next;
    }

    public function count()
    {
        return $this->_stmt->rowCount();
    }

    public function key()
    {
        throw new \BadMethodCallException('Unsupported key operation');
    }

    public function rewind()
    {
        throw new \BadMethodCallException('Unsupported rewind operation');
    }

    public function seek($position)
    {
        throw new \BadMethodCallException('Unsupported seek operation');
    }
}
