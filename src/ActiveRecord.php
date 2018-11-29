<?php
namespace brntsrs\ClickHouse;
use Yii;

class ActiveRecord extends \kak\clickhouse\ActiveRecord
{
    public function insert($runValidation = true, $attributes = null)
    {
        self::getDb()->setToWrite();
        $result = parent::insert($runValidation, $attributes);
        self::getDb()->setToRead();
        return $result;
    }

    /**
     * @return ActiveQuery
     */
    public static function find()
    {
        return Yii::createObject(ActiveQuery::class, [get_called_class()]);
    }
}