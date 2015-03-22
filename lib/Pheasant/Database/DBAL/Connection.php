<?php

namespace Pheasant\Database\DBAL;

use Doctrine\DBAL\Connection as DBALConnection;
use Pheasant\Database\Binder;
use Pheasant\Database\FilterChain;
use Pheasant\Database\MysqlPlatform;

/**
 * A connection to a MySql database
 */
class Connection
{
    private
        $_connection,
        $_filter,
        $_sequencePool,
        $_debug = false;

    public static
        $counter = 0,
        $timer = 0;

    public function __construct(DBALConnection $connection)
    {
        $this->_connection = $connection;
        $this->_filter = new FilterChain();
        $this->_debug = getenv('PHEASANT_DEBUG');
    }

    /**
     * Selects a particular database
     * @chainable
     */
    public function selectDatabase($database)
    {
        $this->_connection->executeQuery("USE $database");

        return $this;
    }

    /**
     * Forces a connection, re-connects if already connected
     * @chainable
     */
    public function connect()
    {
        if ($this->_connection->isConnected()) {
            $this->_connection->close();
        }

        $this->_connection->connect();

        return $this;
    }

    /**
     * Closes a connection
     * @chainable
     */
    public function close()
    {
        $this->_connection->close();

        if ($this->_sequencePool) {
            $this->_sequencePool->close();
        }

        return $this;
    }

    /**
     * The charset used by the database connection
     * @return string
     */
    public function charset()
    {
        // hardcoded?
        return 'utf8';
    }

    /**
     * Executes a statement
     * @return ResultSet
     */
    public function execute($sql, $params = array())
    {
        if (!is_array($params))
            $params = array_slice(func_get_args(), 1);

        $connection = $this->_connection;
        $debug = $this->_debug;

        // delegate execution to the filter chain
        return $this->_filter->execute($sql, function ($sql) use ($params, $connection, $debug) {
            \Pheasant\Database\DBAL\Connection::$counter++;

            if ($debug) {
                $timer = microtime(true);
            }

            $stmt = null;
            $affectedRows = 0;
            $lastInsertId = null;
            if (true) {
                $stmt = $connection->executeQuery($sql, $params);
            } else {
                $affectedRows = $connection->executeUpdate($sql, $params);
                $lastInsertId = $connection->lastInsertId();
            }

            if ($debug) {
                \Pheasant\Database\DBAL\Connection::$timer += microtime(true) - $timer;

                printf(
                    "<pre>Pheasant executed <code>%s</code>  in %.2fms, returned %d rows</pre>\n\n",
                    $sql,
                    (microtime(true) - $timer) * 1000,
                    isset($stmt) ? $stmt->rowCount() : 0
                );
            }

            if ($connection->errorCode()) {
                if ($connection->errorCode() === 1213 || $connection->errorCode() === 1479) {
                    throw new DeadlockException($connection->errorInfo(), $connection->errorCode());
                } else {
                    throw new Exception($connection->errorInfo(), $connection->errorCode());
                }
            }

            return new ResultSet(
                $stmt,
                $affectedRows,
                $lastInsertId
            );
        });
    }

    /**
     * @return Transaction
     */
    public function transaction($callback = null)
    {
        $transaction = new Transaction($this);

        // optionally add a callback and any arguments
        if (func_num_args()) {
            call_user_func_array(array($transaction, 'callback'),
                func_get_args());
        }

        return $transaction;
    }

    /**
     * @return Binder
     */
    public function binder()
    {
        return new Binder();
    }

    /**
     * @return Table
     */
    public function table($name)
    {
        $tablename = new TableName($name);
        if (is_null($tablename->database))
            $tablename->database = $this->selectedDatabase();

        return new Table($tablename, $this);
    }

    /**
     * @return SequencePool
     */
    public function sequencePool()
    {
        if (!isset($this->_sequencePool)) {
            // use a seperate connection, ensures transaction rollback
            // doesn't clobber sequences

            $dbalConnection = new DBALConnection(
                $this->_connection->getParams(),
                $this->_connection->getDriver()
            );
            $this->_sequencePool = new SequencePool(new self($dbalConnection));
        }

        return $this->_sequencePool;
    }

    /**
     * Returns a platform object representing the database type connected to
     */
    public function platform()
    {
        return new MysqlPlatform();
    }

    /**
     * Returns the internal filter chain
     * @return FilterChain
     */
    public function filterChain()
    {
        return $this->_filter;
    }

    /**
     * Returns the selected database
     * @return string
     */
    public function selectedDatabase()
    {
        return $this->_connection->getDatabase();
    }

    public function beginTransaction()
    {
        $this->_connection->beginTransaction();
    }

    public function commit()
    {
        $this->_connection->commit();
    }

    public function rollBack()
    {
        $this->_connection->rollBack();
    }
}
