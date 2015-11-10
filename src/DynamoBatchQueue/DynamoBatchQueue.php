<?php

/**
 * Class DynamoBatchQueue
 * @author Paul L. McNeely (pmcneely@franklinamerican.com)
 */
class DynamoBatchQueue
{
    /**
     * @const BATCH_LIMIT
     */
    const BATCH_LIMIT = 25;
    /**
     * @var \Aws\DynamoDb\DynamoDBClient $dynamoDB
     */
    private $dynamoDB;
    /**
     * @var array $queue
     */
    private $queue;

    /**
     * DynamoDBQueue constructor.
     *
     * @param $aws
     */
    public function __construct($aws)
    {
        /** @var \Aws\Common\Aws $aws */
        $this->dynamoDB = $aws->get('DynamoDb');
    }

    /**
     * @param $table
     * @param $request
     */
    public function enqueuePut($table, $request)
    {
        $this->enqueue($table, "PutRequest", $request);
    }

    /**
     * @param $table
     * @param $request
     */
    public function enqueueDelete($table, $request)
    {
        $this->enqueue($table, "DeleteRequest", $request);
    }

    /**
     * @param $table
     * @param $requestType
     * @param $request
     */
    private function enqueue($table, $requestType, $request)
    {
        $this->queue[$table][] = [$requestType => $request];
        $this->dequeue($table);
    }

    /**
     * @param mixed      $table
     * @param bool|false $force
     */
    public function dequeue($table = false, $force = false)
    {
        if ($table) {
            if ((count($this->queue[$table]) >= self::BATCH_LIMIT) || (true == $force)) {
                $batch = [];
                $limit = $force ? 1 : self::BATCH_LIMIT;
                while (count($this->queue[$table]) >= $limit) {
                    for($batchInt = 1; $batchInt<= self::BATCH_LIMIT, $batchInt++) {
                        $batch[] = array_shift($this->queue[$table]);
                    }
                    $this->dynamoDB->batchWriteItem(['RequestItems' => [$table => $batch]]);
                    $batch = [];
                }
            }
        } else {
            foreach ($this->queue as $key => $value) {
                $this->dequeue($key, true);
            }
        }
    }

}