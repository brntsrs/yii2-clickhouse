<?php
namespace brntsrs\ClickHouse;

use kak\clickhouse\Query;

class ActiveQuery extends \kak\clickhouse\ActiveQuery
{
    /**
     * {@inheritdoc}
     */
    public function populate($rows)
    {
        if (empty($rows)) {
            return [];
        }

        $models = $this->createModels($rows);
        if (!empty($this->with)) {
            $this->findWith($this->with, $models);
        }

        if (!$this->asArray) {
            foreach ($models as $model) {
                $model->afterFind();
            }
        }

        return parent::populate($models);
    }

    public function count($q = '', $db = null)
    {
        if (
            !$this->distinct
            && empty($this->groupBy)
            && empty($this->having)
            && empty($this->union)
        ) {
            return parent::count($q, $db);
        }
        $command = (new Query())
            ->select(["COUNT($q)"])
            ->from(['c' => $this])
            ->createCommand($db);
        $this->setCommandCache($command);

        return $command->queryScalar();
    }
}