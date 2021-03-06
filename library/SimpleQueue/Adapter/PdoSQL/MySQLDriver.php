<?php
namespace SimpleQueue\Adapter\PdoSQL;

/**
 * Class MysqlPdoDriver
 * @package SimpleQueue\Adapter\Sql
 * @author Aleksey Pustovalov (alpust@gmail.com)
 * @license GPLv2
 */
class MySQLDriver implements DriverInterface {

    /**
     * @var \PDO
     */
    protected $connection;

    /**
     * @var array
     */
    protected $config;

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
        $this->config = [
            'dsn' => "mysql:dbname=" . (string) $databaseName . ";host=" . (string) $host,
            'user' => $user,
            'password' => $password
        ];
    }

    /**
     * @return bool
     */
    public function isQueueTableVerified()
    {
        try {
            $request = $this->getConnection()->prepare("describe " . self::TABLE_NAMESPACE . self::QUEUES_TABLE);
            $request->execute();

            $rows = $request->fetchAll(\PDO::FETCH_ASSOC);

            if (!isset($rows[0]['Field']) || $rows[0]['Field'] != "id") {
                return false;
            }

            if (!isset($rows[1]['Field']) || $rows[1]['Field'] != "queue_name") {
                return false;
            }

            return true;
        } catch(\Exception $e) {
            return false;
        }
    }

    /**
     * @return string
     */
    public function createQueueTable()
    {
        $sql = "CREATE TABLE "
            . self::TABLE_NAMESPACE . self::QUEUES_TABLE
            . "(id INT AUTO_INCREMENT NOT NULL, queue_name VARCHAR(1024) NOT NULL, PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = MyIsam";

        $this->getConnection()->exec($sql);

        return true;
    }

    /**
     * @return bool
     */
    public function isMessageTableVerified()
    {
        try {
            $request = $this->getConnection()->prepare("describe " . self::TABLE_NAMESPACE . self::MESSAGES_TABLE);
            $request->execute();

            $rows = $request->fetchAll(\PDO::FETCH_ASSOC);

            if (!isset($rows[0]['Field']) || $rows[0]['Field'] != "id") {
                return false;
            }

            if (!isset($rows[1]['Field']) || $rows[1]['Field'] != "queue_id") {
                return false;
            }

            if (!isset($rows[2]['Field']) || $rows[2]['Field'] != "subject") {
                return false;
            }

            if (!isset($rows[3]['Field']) || $rows[3]['Field'] != "type") {
                return false;
            }

            return true;
        } catch(\Exception $e) {
            return false;
        }
    }

    /**
     * @return bool
     */
    public function createMessageTable()
    {
        $sql = "CREATE TABLE "
            . self::TABLE_NAMESPACE . self::MESSAGES_TABLE
            . "(id INT AUTO_INCREMENT NOT NULL, queue_id INT NOT NULL, subject VARCHAR(2048) NOT NULL, `type` VARCHAR(128) NOT NULL, INDEX messages_queue_id (queue_id), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB";

        $this->getConnection()->exec($sql);

        return true;
    }

    /**
     * @param $queueName
     * @return bool
     */
    public function isExist($queueName)
    {
        $sql = "select id from " . self::TABLE_NAMESPACE . self::QUEUES_TABLE . " where queue_name = :queueName";
        $request = $this->getConnection()->prepare($sql);
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
        $request = $this->getConnection()->prepare($sql);
        $request->execute(["queueName" => $queueName]);

        return $this->getConnection()->lastInsertId();
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
        $request = $this->getConnection()->prepare($sql);
        $request->execute(["queueId" => $queueId, 'subject' => $subject, 'type' => $type]);

        return $this->getConnection()->lastInsertId();
    }

    /**
     * @param $queueId
     * @return mixed|null
     */
    public function getMessage($queueId)
    {
        try {
            $this->getConnection()->beginTransaction();
            $sql = "SELECT * FROM " . self::TABLE_NAMESPACE . self::MESSAGES_TABLE . " WHERE queue_id = :queueId ORDER BY id ASC LIMIT 1 FOR UPDATE";
            $request = $this->getConnection()->prepare($sql);
            $request->execute(['queueId' => $queueId]);
            $message = $request->fetch(\PDO::FETCH_ASSOC);

            if (!isset($message['id'])) {
                $this->getConnection()->rollBack();
                return null;
            }

            $sql = "DELETE FROM " . self::TABLE_NAMESPACE . self::MESSAGES_TABLE . " WHERE id = :id";
            $request = $this->getConnection()->prepare($sql);
            $request->execute(['id' => $message['id']]);

            $this->getConnection()->commit();
            return $message;
        } catch(\Exception $e) {
            $this->getConnection()->rollBack();
        }

        return null;
    }

    public function __destruct()
    {
        $this->connection = null;
    }

    /**
     * @return \PDO
     */
    protected function getConnection()
    {
        if(!is_null($this->connection)) {
            return $this->connection;
        }

        $this->connection = new \PDO($this->config['dsn'], $this->config['user'], $this->config['password']);
        $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $this->connection;
    }

    /**
     * @return $this
     */
    public function closeConnection()
    {
        $this->connection = null;

        return $this;
    }
}