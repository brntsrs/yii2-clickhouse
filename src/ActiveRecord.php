<?php
namespace brntsrs\ClickHouse;
use kak\clickhouse\ColumnSchema;
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

    private function prepareAttributeValue($attribute, $value)
    {
        switch ($this->getAttributeType($attribute)) {
            case 'integer':
                return is_numeric($value) || empty($value) ? intval($value) : $value;
                break;
            case 'float':
                return is_numeric($value) || empty($value) ? floatval($value) : $value;
                break;
            case 'boolean':
                return boolval($value) ? 1 : 0;
                break;
            case 'array':
                return (array)$value;
                break;
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

        return self::getTableSchema()->getColumn($attribute)->phpType;
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
}