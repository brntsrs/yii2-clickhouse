<?php
namespace brntsrs\ClickHouse;

class Command extends \kak\clickhouse\Command
{
    public function bindValues($values)
    {
        if (empty($values)) {
            return $this;
        }
        //$schema = $this->db->getSchema();
        foreach ($values as $name => $value) {
            if (is_array($value)) {
                $this->params[$name] = $value;
            } else {
                $this->params[$name] = $value;
            }
        }

        return $this;
    }

    public function getRawSql()
    {
        if (empty($this->params)) {
            return $this->getSql();
        }
        $params = [];
        foreach ($this->params as $name => $value) {
            if (is_string($name) && strncmp(':', $name, 1)) {
                $name = ':' . $name;
            }

            if (is_string($value)) {
                $params[$name] = $this->db->quoteValue($value);
            } elseif (is_bool($value)) {
                $params[$name] = ($value ? 'TRUE' : 'FALSE');
            } elseif ($value === null) {
                $params[$name] = 'NULL';
            } elseif (is_array($value)) {
                $data = [];
                foreach ($value as $item) {
                    $data[] = $this->db->quoteValue($item);
                }
                $params[$name] = '[' . implode(', ', $data) . ']';
            } elseif (!is_object($value) && !is_resource($value)) {
                $params[$name] = $value;
            }
        }
        if (!isset($params[1])) {
            return strtr($this->getSql(), $params);
        }
        $sql = '';
        foreach (explode('?', $this->getSql()) as $i => $part) {
            $sql .= (isset($params[$i]) ? $params[$i] : '') . $part;
        }
        return $sql;
    }

    public function changeOptions($table, $options)
    {
        $schema = $this->db->getSchema()->getTableSchema($table, true);
        $columns = [];
        $columnNames = [];
        $structures = [];
        foreach ($schema->columns as $column) {
            if (strpos($column->name, '.') !== false) {
                $name = explode('.', $column->name);
                if (!isset($structures[$name[0]])) {
                    $structures[$name[0]] = [];
                }
                $structures[$name[0]][] = $name[1] . ' ' . trim(str_replace('Array', '', $column->dbType), ')(');
            } else {
                $columns[$column->name] = $this->db->getSchema()->createColumnSchemaBuilder($column->type);
            }
            $columnNames[] = $column->name;
        }
        foreach ($structures as $columnName => $structure) {
            $columns[$columnName] = 'Nested (' . implode(', ', $structure) . ')';
        }
        $this->createTable($table . '_new', $columns, $options)->execute();
        $this->db->createCommand('INSERT INTO `' . $table . '_new` (' . implode(', ', $columnNames) . ') SELECT ' . implode(', ', $columnNames) . ' FROM `' . $table . '`')->execute();
        $this->renameTable($table, $table . '_old')->execute();
        $this->renameTable($table . '_new', $table)->execute();
        $this->dropTable($table . '_old')->execute();

        return true;
    }
}