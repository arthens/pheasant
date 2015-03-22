<?php

namespace Pheasant\Database\DBAL;

/**
 * Encapsulates the result of executing a statement
 */
class ResultSet implements \IteratorAggregate, \ArrayAccess, \Countable
{
    private $_stmt;
    private $_affectedRows;
    private $_lastInsertId;
    private $_hydrator;

    public function __construct($stmt = null, $affectedRows = 0, $lastInsertId = null)
    {
        $this->_stmt = $stmt;
        $this->_affectedRows = $affectedRows;
        $this->_lastInsertId = $lastInsertId;
    }

    public function setHydrator($callback)
    {
        $this->_hydrator = $callback;

        return $this;
    }

    public function getIterator()
    {
        if (!isset($this->_iterator)) {
            $this->_iterator = new ResultIterator($this->_stmt);
            $this->_iterator->setHydrator($this->_hydrator);
        }

        return $this->_iterator;
    }

    /**
     * @return array
     */
    public function toArray()
    {
        return iterator_to_array($this->getIterator());
    }

    /**
     * Returns the next available row as an associative array.
     * @return array or NULL on EOF
     */
    public function row()
    {
        $iterator = $this->getIterator();

        if(!$iterator->current())
            $iterator->next();

        $value = $iterator->current();
        $iterator->next();

        return $value;
    }

    /**
     * Returns the nth column from the current row.
     * @return scalar or NULL on EOF
     */
    public function scalar($idx=0)
    {
        $row = $this->row();

        if(is_null($row))

            return NULL;

        $values = is_numeric($idx) ? array_values($row) : $row;

        return $values[$idx];
    }

    /**
     * Fetches an iterator that only returns a particular column, defaults to the
     * first
     * @return \Iterator
     */
    public function column($column=null)
    {
        return new ColumnIterator($this->getIterator(), $column);
    }

    /**
     * Seeks to a particular row offset
     * @chainable
     */
    public function seek($offset)
    {
        $this->getIterator()->seek($offset);

        return $this;
    }

    /**
     * The number of rows that the statement affected
     * @return int
     */
    public function affectedRows()
    {
        return $this->_affectedRows;
    }

    /**
     * The fields returned in the result set as an array of fields
     * @return Fields object
     */
    public function fields()
    {
        throw new \BadMethodCallException('Unsupported fields() operation');
    }

    /**
     * The number of rows in the result set, or the number of affected rows
     */
    public function count()
    {
        return $this->_affectedRows;
    }

    /**
     * The last auto_increment value generated in the statement
     */
    public function lastInsertId()
    {
        return $this->_lastInsertId;
    }

    // ----------------------------------
    // array access

    public function offsetGet($offset)
    {
        $this->getIterator()->seek($offset);

        return $this->getIterator()->current();
    }

    public function offsetSet($offset, $value)
    {
        throw new \BadMethodCallException('ResultSets are read-only');
    }

    public function offsetExists($offset)
    {
        $this->getIterator()->seek($offset);

        return $this->getIterator()->valid();
    }

    public function offsetUnset($offset)
    {
        throw new \BadMethodCallException('ResultSets are read-only');
    }
}
