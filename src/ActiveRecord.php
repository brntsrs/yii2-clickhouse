<?php
namespace brntsrs\ClickHouse;
use kak\clickhouse\ColumnSchema;
use Yii;

class ActiveRecord extends \kak\clickhouse\ActiveRecord
{
    protected $isForSearch = false;

    public function insert($runValidation = true, $attributes = null)
    {
        if ($runValidation && !$this->validate($attributes)) {
            Yii::info('Model not inserted due to validation error.', __METHOD__);
            return false;
        }
        if (!$this->beforeSave(true)) {
            return false;
        }

        $values = $this->getDirtyAttributes($attributes);
        self::getDb()->setToWrite();
        if ((static::getDb()->getSchema()->insert(static::tableName(), $values)) === false) {
            self::getDb()->setToRead();
            return false;
        }
        self::getDb()->setToRead();

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);
        $this->afterSave(true, $changedAttributes);
        return true;

    }

    /**
     * @return ActiveQuery
     */
    public static function find()
    {
        return Yii::createObject(ActiveQuery::class, [get_called_class()]);
    }

    public function beforeSave($insert)
    {
        foreach ($this->attributes as $attribute => $value) {
            if (empty($value)) {
                $this->$attribute = $this->prepareAttributeValue($attribute, $value);
            }
        }

        return parent::beforeSave($insert);
    }

    public function __set($name, $value)
    {
        if ($this->hasAttribute($name)) {
            parent::__set($name, $this->prepareAttributeValue($name, $value));
        } else {
            parent::__set($name, $value);
        }
    }

    public function setAttributes($values, $safeOnly = true)
    {
        if (is_array($values)) {
            $attributes = array_flip($safeOnly ? $this->safeAttributes() : $this->attributes());
            foreach ($values as $name => $value) {
                if (isset($attributes[$name])) {
                    $this->$name = $this->prepareAttributeValue($name, $value);
                } elseif ($safeOnly) {
                    $this->onUnsafeAttribute($name, $value);
                }
            }
        }
    }

    public function setSearchMode($isEnabled = true)
    {
        $this->isForSearch = $isEnabled;
    }

    private function prepareAttributeValue($attribute, $value)
    {
        if ($this->isForSearch && ($value === '' || $value === null || $value === [])) {
            return null;
        }
        $attributeType = $this->getAttributeType($attribute);
        $conversionTypes = ['integer', 'float', 'double', 'boolean', 'date', 'string'];
        if (is_array($value) && in_array($attributeType, $conversionTypes)) {
            $list = [];
            foreach ($value as $id => $data) {
                $list[$id] = $this->prepareAttributeValue($attribute, $data);
            }
            return $list;
        }
        switch ($attributeType) {
            case 'integer':
                return is_numeric($value) || empty($value) ? intval($value) : $value;
                break;
            case 'float':
                return is_numeric($value) || empty($value) ? floatval($value) : $value;
                break;
            case 'double':
                return is_numeric($value) || empty($value) ? doubleval($value) : $value;
                break;
            case 'boolean':
                return boolval($value) ? 1 : 0;
                break;
            case 'array':
                return (array)$value;
                break;
            case 'date':
                return empty($value) ? '0000-00-00' : $value;
            case 'safe':
                return $value;
        }

        return strval($value);
    }

    private function getAttributeType($attribute)
    {
        foreach ($this->rules() as $rule) {
            if (is_array($rule[0])) {
                foreach ($rule[0] as $name) {
                    if ($name == $attribute) {
                        $type = $this->getRuleType($rule);
                        if (!empty($type)) {
                            return $type;
                        }
                    }
                }
            } else {
                if ($rule[0] == $attribute) {
                    $type = $this->getRuleType($rule);
                    if (!empty($type)) {
                        return $type;
                    }
                }
            }
        }

        $column = self::getTableSchema()->getColumn($attribute);
        if ($column->type == 'date') {
            return 'date';
        }
        if (strpos($column->dbType, 'Array') !== false) {
            return 'array';
        }
        return $column->phpType;
    }

    private function getRuleType($rule)
    {
        $types = [
            'string' => 'string',
            'number' => 'float',
            'integer' => 'integer',
            'boolean' => 'boolean',
            'in' => 'array',
            'safe' => 'safe',
        ];

        return isset($types[$rule[1]]) ? $types[$rule[1]] : null;
    }



    public function getProperties()
    {
        $class = new \ReflectionClass($this);
        $names = [];
        foreach ($class->getProperties(\ReflectionProperty::IS_PUBLIC) as $property) {
            if (!$property->isStatic()) {
                $names[] = $property->getName();
            }
        }

        return $names;
    }

    /**
     * @param ActiveRecord $record
     * @param array|ActiveRecord $row
     */
    public static function populateRecord($record, $row)
    {
        $columns = array_flip($record->attributes());
        if (!is_array($row)) {
            foreach ($record->getProperties() as $property) {
                if (isset($row->{$property})) {
                    $record->{$property} = $row->{$property};
                }
            }
        }

        parent::populateRecord($record, $row);
    }
}