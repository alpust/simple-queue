<?php
namespace SimpleQueue\Adapter;

use SimpleQueue\Adapter\PdoSQL\DriverInterface;
use SimpleQueue\AdapterInterface;

/**
 * Class PdoSQLAdapter
 * @package SimpleQueue\Adapter
 * @author Aleksey Pustovalov (alpust@gmail.com)
 * @license GPLv2
 */
class PdoSQLAdapter implements AdapterInterface {

    /**
     * @var array
     */
    protected $existQueues = [];

    /**
     * @var boolean
     */
    protected $queueTableVerified;

    /**
     * @var boolean
     */
    protected $messageTableVerified;

    /**
     * @var DriverInterface
     */
    protected $driver;

    /**
     * @param DriverInterface $driver
     */
    public function __construct(DriverInterface $driver)
    {
        $this->driver = $driver;
    }


    /**
     * @param $queueName
     * @param $subject
     * @param $type
     * @return mixed
     */
    public function enqueue($queueName, $subject, $type)
    {
        if(!$this->isExist($queueName)) {
            $this->existQueues[$queueName] = $this->driver->createQueue($queueName);
        }

        $queueId = $this->existQueues[$queueName];

        $this->isMessageTableVerified();

        return $this->driver->insertMessage($queueId, $subject, $type);
    }


    /**
     * @param $queueName
     * @return mixed|null
     */
    public function dequeue($queueName)
    {
        if(!$this->isExist($queueName)) {
            return null;
        }

        $queueId = $this->existQueues[$queueName];
        $message = $this->driver->getMessage($queueId);

        if(isset($message['id'])) {
            switch($message['type']) {
                case "object":
                case "array":
                    return unserialize($message['subject']);
                    break;
                default:
                    return $message['subject'];
            }
        }

        return null;
    }

    /**
     * @param $queueName
     * @return bool
     */
    protected function isExist($queueName)
    {
        if(isset($this->existQueues[$queueName])) {
            return true;
        }

        $this->isQueueTableVerified();

        $queueId = $this->driver->isExist($queueName);

        if($queueId > 0) {
            $this->existQueues[$queueName] = $queueId;
            return true;
        }

        return false;
    }

    /**
     * @return bool
     */
    protected function isQueueTableVerified()
    {
        if(!is_null($this->queueTableVerified)) {
            return $this->queueTableVerified;
        }

        $this->queueTableVerified = $this->driver->isQueueTableVerified();

        if(!$this->queueTableVerified) {
            $this->driver->createQueueTable();
            $this->queueTableVerified = true;
        }

        return $this->queueTableVerified;
    }

    /**
     * @return bool
     */
    protected function isMessageTableVerified()
    {
        if(!is_null($this->messageTableVerified)) {
            return $this->messageTableVerified;
        }

        $this->messageTableVerified = $this->driver->isMessageTableVerified();

        if(!$this->messageTableVerified) {
            $this->driver->createMessageTable();
            $this->messageTableVerified = true;
        }

        return $this->messageTableVerified;
    }

    public function __destruct()
    {
        unset($this->driver);
    }

    /**
     * Close connection to db
     * @return $this
     */
    public function closeConnection()
    {
        $this->driver->closeConnection();

        return $this;
    }

    /**
     * Close connection, delete references and another data, which required some actions
     * @return mixed
     */
    public function closeExternalResources()
    {
        return $this->driver->closeConnection();
    }
}