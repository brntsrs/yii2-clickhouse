<?php
namespace brntsrs\ClickHouse;

use kak\clickhouse\ColumnSchema;

class Schema extends \kak\clickhouse\Schema
{
    const TYPE_ARRAY = 'array';

    /**
     * Loads the column information into a [[ColumnSchema]] object.
     * @param array $info column information
     * @return ColumnSchema the column schema object
     */
    protected function loadColumnSchema($info)
    {
        $column = $this->createColumnSchema();
        $column->name = $info['name'];
        $column->dbType = $info['type'];
        $column->type = isset($this->typeMap[$column->dbType]) ? $this->typeMap[$column->dbType] : self::TYPE_STRING;

        if (strpos($column->dbType, 'Array') !== false) {
            $column->type = self::TYPE_JSON;
        }

        if (preg_match('/^([\w ]+)(?:\(([^\)]+)\))?$/', $column->dbType, $matches)) {
            $type = $matches[1];
            $column->dbType = $matches[1] . (isset($matches[2]) ? "({$matches[2]})" : '');
            if (isset($this->typeMap[$type])) {
                $column->type = $this->typeMap[$type];
            }
        }

        $unsignedTypes = ['UInt8', 'UInt16', 'UInt32', 'UInt64'];
        if(in_array($column->dbType, $unsignedTypes)) {
            $column->unsigned = true;
        }

        $column->phpType = $this->getColumnPhpType($column);
        if (empty($info['default_type'])) {
            $column->defaultValue = $info['default_expression'];
        }
        return $column;
    }
}