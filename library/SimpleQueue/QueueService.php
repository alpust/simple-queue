<?php
namespace SimpleQueue;

/**
 * Class QueueService
 * @package SimpleQueue
 * @author Aleksey Pustovalov (alpust@gmail.com)
 * @license GPLv2
 */
class QueueService {

    /**
     * @var AdapterInterface
     */
    protected $adapter;

    /**
     * @param AdapterInterface $adapter
     */
    public function __construct(AdapterInterface $adapter)
    {
        $this->adapter = $adapter;
    }

    /**
     * @param $queueName
     * @param $subject
     * @return mixed
     */
    public function enqueue($queueName, $subject)
    {
        $type = gettype($subject);

        if($type == "object" || $type == "array") {
            $subject = serialize($subject);
        }

        return $this->adapter->enqueue($queueName, $subject, $type);
    }

    /**
     * @param $queueName
     * @return mixed
     */
    public function dequeue($queueName)
    {
        return $this->adapter->dequeue($queueName);
    }

    /**
     *
     */
    public function __destruct()
    {
        unset($this->adapter);
    }

    /**
     * @return mixed
     */
    public function closeExternalResources()
    {
        return $this->adapter->closeExternalResources();
    }
}