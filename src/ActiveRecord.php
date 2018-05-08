<?php
namespace brntsrs\ClickHouse;

class ActiveRecord extends \kak\clickhouse\ActiveRecord
{
    public function insert($runValidation = true, $attributes = null)
    {
        self::getDb()->setToWrite();
        $result = parent::insert($runValidation, $attributes);
        self::getDb()->setToRead();
        return $result;
    }
}