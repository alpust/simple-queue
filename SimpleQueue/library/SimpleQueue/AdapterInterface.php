<?php
namespace SimpleQueue;

/**
 * Class AdapterInterface
 * @package SimpleQueue
 * @author Aleksey Pustovalov (alpust@gmail.com)
 * @license GPLv2
 */
interface AdapterInterface {

    /**
     * @param $queueName
     * @param $message
     * @param $type
     * @return mixed
     */
    public function enqueue($queueName, $message, $type);

    /**
     * @param $queueName
     * @return mixed
     */
    public function dequeue($queueName);
}