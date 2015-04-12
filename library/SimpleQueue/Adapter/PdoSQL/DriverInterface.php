<?php
namespace SimpleQueue\Adapter\PdoSql;

/**
 * Interface DriverInterface
 * @package SimpleQueue\Adapter\Sql
 */
interface DriverInterface {

    /**
     * @return boolean
     */
    public function isQueueTableVerified();

    /**
     * @return boolean
     */
    public function isMessageTableVerified();

    /**
     * @param $queueName
     * @return mixed
     */
    public function isExist($queueName);

    /**
     * @param $queueName
     * @return integer
     */
    public function createQueue($queueName);

    /**
     * @param $queueId
     * @param $subject
     * @param $type
     * @return mixed
     */
    public function insertMessage($queueId, $subject, $type);

    /**
     * @return mixed
     */
    public function createQueueTable();

    /**
     * @return mixed
     */
    public function createMessageTable();

    /**
     * @param $queueId
     * @return mixed
     */
    public function getMessage($queueId);

}