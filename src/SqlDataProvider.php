<?php
namespace brntsrs\ClickHouse;

use yii\db\Expression;

class SqlDataProvider extends \kak\clickhouse\data\SqlDataProvider
{
    /**
     * @var \kak\clickhouse\Command
     */
    private $_command;

    protected function prepareModels()
    {
        $sort = $this->getSort();
        $pagination = $this->getPagination();
        if ($pagination === false && $sort === false) {
            $this->_command = $this->db->createCommand($this->sql, $this->params);
            return $this->_command->queryAll();
        }

        $sql = $this->sql;
        $orders = [];
        $limit = $offset = null;

        if ($sort !== false) {
            $orders = $sort->getOrders();
            $pattern = '/\s+order\s+by\s+([\w\s,\.]+)$/i';
            if (preg_match($pattern, $sql, $matches)) {
                array_unshift($orders, new Expression($matches[1]));
                $sql = preg_replace($pattern, '', $sql);
            }
        }
        if ($pagination !== false) {
            if (!$page = (int)\Yii::$app->request->get($pagination->pageParam,0)) {
                $page = 1;
            }
            $pagination->totalCount = $page * $pagination->getPageSize();
            $limit = $pagination->getLimit();
            $offset = $pagination->getOffset();
        }

        $sql = $this->db->getQueryBuilder()->buildOrderByAndLimit($sql, $orders, $limit, $offset);

        $this->_command = $this->db->createCommand($sql, $this->params);
        $result = $this->_command->queryAll();

        if ($pagination !== false) {
            $pagination->totalCount = $this->getTotalCount();
            $pagination->getPageSize();
            $this->setTotalCount( $pagination->totalCount );
        }
        return $result;
    }

    protected function prepareTotalCount()
    {
        return (new \kak\clickhouse\Query([
            'from' => ['sub' => "({$this->sql})"],
            'params' => $this->params,
        ]))->count('*');
    }
}