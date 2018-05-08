<?php
namespace brntsrs\ClickHouse;

class Dictionary
{
    static public function getDbPrefix()
    {
        return \Yii::$app->id . '_';
    }

    static public function getDictName($name)
    {
        return self::getDbPrefix() . $name;
    }

    static public function getDictSelector($dictName, $dataType, $dataField, $sourceField)
    {
        return 'dictGet' . $dataType . '(\'' . self::getDictName($dictName) .'\', \'' . $dataField . '\', toUInt64(' . $sourceField . '))';
    }
}