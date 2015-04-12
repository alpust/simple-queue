<?php
namespace SimpleQueue\Adapter\PdoSQL;

/**
 * Class MysqlPdoDriver
 * @package SimpleQueue\Adapter\Sql
 */
class MySQLDriver implements DriverInterface {

    /**
     * @var \PDO
     */
    protected $connection;

    const TABLE_NAMESPACE = "simple_queue_";

    const QUEUES_TABLE = "queues";

    const MESSAGES_TABLE = "messages";


    /**
     * @param $host
     * @param $user
     * @param $password
     * @param $databaseName
     */
    public function __construct($host, $user, $password, $databaseName)
    {
        $dsn = "mysql:dbname=" . (string) $databaseName . ";host=" . (string) $host;
        $this->connection = new \PDO($dsn, $user, $password);
        $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    /**
     * @return bool
     */
    public function isQueueTableVerified()
    {
        $request = $this->connection->prepare("describe " . self::TABLE_NAMESPACE . self::QUEUES_TABLE);
        $request->execute();

        $rows = $request->fetchAll(\PDO::FETCH_ASSOC);

        if(!isset($rows[0]['field']) || $rows[0]['field'] != "id") {
            return false;
        }

        if(!isset($rows[1]['field']) || $rows[0]['field'] != "queue_name") {
            return false;
        }

        return true;
    }

    /**
     * @return string
     */
    public function createQueueTable()
    {
        $sql = "CREATE TABLE "
            . self::TABLE_NAMESPACE . self::QUEUES_TABLE
            . "(id INT AUTO_INCREMENT NOT NULL, queue_name VARCHAR(1024) NOT NULL, PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = MyIsam";

        $this->connection->exec($sql);

        return true;
    }

    /**
     * @return bool
     */
    public function isMessageTableVerified()
    {
        $request = $this->connection->prepare("describe " . self::TABLE_NAMESPACE . self::MESSAGES_TABLE);
        $request->execute();

        $rows = $request->fetchAll(\PDO::FETCH_ASSOC);

        if(!isset($rows[0]['field']) || $rows[0]['field'] != "id") {
            return false;
        }

        if(!isset($rows[1]['field']) || $rows[0]['field'] != "queue_id") {
            return false;
        }

        if(!isset($rows[2]['field']) || $rows[0]['field'] != "subject") {
            return false;
        }

        if(!isset($rows[3]['field']) || $rows[0]['field'] != "type") {
            return false;
        }

        return true;
    }

    /**
     * @return bool
     */
    public function createMessageTable()
    {
        $sql = "CREATE TABLE "
            . self::TABLE_NAMESPACE . self::MESSAGES_TABLE
            . "(id INT AUTO_INCREMENT NOT NULL, queue_id INT NOT NULL, subject VARCHAR(2048) NOT NULL, `type` VARCHAR(128) NOT NULL, INDEX messages_queue_id (queue_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB";

        $this->connection->exec($sql);

        return true;
    }

    /**
     * @param $queueName
     * @return bool
     */
    public function isExist($queueName)
    {
        $sql = "select id from " . self::TABLE_NAMESPACE . self::QUEUES_TABLE . " where queue_name = :queueName";
        $request = $this->connection->prepare($sql);
        $request->execute(['queueName' => $queueName]);
        return $request->fetchColumn();
    }

    /**
     * @param $queueName
     * @return string
     */
    public function createQueue($queueName)
    {
        $sql = "insert into " . self::TABLE_NAMESPACE . self::QUEUES_TABLE . " values(null, :queueName)";
        $request = $this->connection->prepare($sql);
        $request->execute(["queueName" => $queueName]);

        return $this->connection->lastInsertId();
    }

    /**
     * @param $queueId
     * @param $subject
     * @param $type
     * @return string
     */
    public function insertMessage($queueId, $subject, $type)
    {
        $sql = "insert into " . self::TABLE_NAMESPACE . self::MESSAGES_TABLE . " values(NULL, :queueId, :subject, :type)";
        $request = $this->connection->prepare($sql);
        $request->execute(["queueId" => $queueId, 'subject' => $subject, 'type' => $type]);

        return $this->connection->lastInsertId();
    }

    /**
     * @param $queueId
     * @return mixed|null
     */
    public function getMessage($queueId)
    {
        try {
            $this->connection->beginTransaction();
            $sql = "SELECT * FROM " . self::TABLE_NAMESPACE . self::MESSAGES_TABLE . " WHERE queue_id = :queueId ORDER BY id ASC LIMIT 1 FOR UPDATE";
            $request = $this->connection->prepare($sql);
            $request->execute();
            $message = $request->fetch(\PDO::FETCH_ASSOC);

            if (!isset($message['id'])) {
                $this->connection->rollBack();
                return null;
            }

            $sql = "DELETE FROM " . self::TABLE_NAMESPACE . self::MESSAGES_TABLE . " WHERE id = :id";
            $request = $this->connection->prepare($sql);
            $request->execute(['id' => $message['id']]);

            $this->connection->commit();
            return $message;
        } catch(\Exception $e) {
            $this->connection->rollBack();
        }

        return null;
    }

    public function __destruct()
    {
        unset($this->connection);
    }
}