<?php
namespace brntsrs\ClickHouse;

class Command extends \kak\clickhouse\Command
{
    public function createTable($table, $columns, $options = null)
    {
        if (!empty($this->db->isReplicated)) {
            $options = empty($options) ? $options : str_replace(
                'ENGINE = MergeTree()',
                'ENGINE = ReplicatedMergeTree' . (empty($this->db->replicatedOptions) ? '' : ('(' . $this->db->replicatedOptions . ')')),
                $options
            );
        }

        $sql = $this->db->getQueryBuilder()->createTable($table, $columns, $options);

        if (!empty($this->db->isReplicated)) {
            $sql = str_replace(
                'CREATE TABLE ' . $this->db->quoteTableName($table),
                'CREATE TABLE ' . $this->db->quoteTableName($table) . ' ON CLUSTER ' . $this->db->replicatedClusterName,
                $sql
            );
        }

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    /**
     * Creates a SQL command for renaming a DB table.
     * @param string $table the table to be renamed. The name will be properly quoted by the method.
     * @param string $newName the new table name. The name will be properly quoted by the method.
     * @return \yii\db\Command the command object itself
     */
    public function renameTable($table, $newName)
    {
        $sql = $this->db->getQueryBuilder()->renameTable($table, $newName);

        if (!empty($this->db->isReplicated)) {
            $sql .= ' ON CLUSTER ' . $this->db->replicatedClusterName;
        }

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    /**
     * Creates a SQL command for dropping a DB table.
     * @param string $table the table to be dropped. The name will be properly quoted by the method.
     * @return $this the command object itself
     */
    public function dropTable($table)
    {
        $sql = $this->db->getQueryBuilder()->dropTable($table);

        if (!empty($this->db->isReplicated)) {
            $sql .= ' ON CLUSTER ' . $this->db->replicatedClusterName;
        }

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }


    /**
     * Creates a SQL command for truncating a DB table.
     * @param string $table the table to be truncated. The name will be properly quoted by the method.
     * @return \yii\db\Command the command object itself
     */
    public function truncateTable($table)
    {
        $sql = $this->db->getQueryBuilder()->truncateTable($table);

        if (!empty($this->db->isReplicated)) {
            $sql .= ' ON CLUSTER ' . $this->db->replicatedClusterName;
        }

        return $this->setSql($sql);
    }

    /**
     * Creates a SQL command for adding a new DB column.
     * @param string $table the table that the new column will be added to. The table name will be properly quoted by the method.
     * @param string $column the name of the new column. The name will be properly quoted by the method.
     * @param string $type the column type. [[\yii\db\QueryBuilder::getColumnType()]] will be called
     * to convert the give column type to the physical one. For example, `string` will be converted
     * as `varchar(255)`, and `string not null` becomes `varchar(255) not null`.
     * @return $this the command object itself
     */
    public function addColumn($table, $column, $type)
    {
        $sql = $this->db->getQueryBuilder()->addColumn($table, $column, $type);

        if (!empty($this->db->isReplicated)) {
            $sql = str_replace(
                'ALTER TABLE ' . $this->db->quoteTableName($table),
                'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ON CLUSTER ' . $this->db->replicatedClusterName,
                $sql
            );
        }

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    /**
     * Creates a SQL command for dropping a DB column.
     * @param string $table the table whose column is to be dropped. The name will be properly quoted by the method.
     * @param string $column the name of the column to be dropped. The name will be properly quoted by the method.
     * @return $this the command object itself
     */
    public function dropColumn($table, $column)
    {
        $sql = $this->db->getQueryBuilder()->dropColumn($table, $column);

        if (!empty($this->db->isReplicated)) {
            $sql = str_replace(
                'ALTER TABLE ' . $this->db->quoteTableName($table),
                'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ON CLUSTER ' . $this->db->replicatedClusterName,
                $sql
            );
        }

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    /**
     * Creates a SQL command for renaming a column.
     * @param string $table the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $oldName the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     * @return $this the command object itself
     */
    public function renameColumn($table, $oldName, $newName)
    {
        $sql = $this->db->getQueryBuilder()->renameColumn($table, $oldName, $newName);

        if (!empty($this->db->isReplicated)) {
            $sql = str_replace(
                'ALTER TABLE ' . $this->db->quoteTableName($table),
                'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ON CLUSTER ' . $this->db->replicatedClusterName,
                $sql
            );
        }

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    /**
     * Creates a SQL command for changing the definition of a column.
     * @param string $table the table whose column is to be changed. The table name will be properly quoted by the method.
     * @param string $column the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $type the column type. [[\yii\db\QueryBuilder::getColumnType()]] will be called
     * to convert the give column type to the physical one. For example, `string` will be converted
     * as `varchar(255)`, and `string not null` becomes `varchar(255) not null`.
     * @return $this the command object itself
     */
    public function alterColumn($table, $column, $type)
    {
        $sql = $this->db->getQueryBuilder()->alterColumn($table, $column, $type);

        if (!empty($this->db->isReplicated)) {
            $sql = str_replace(
                'ALTER TABLE ' . $this->db->quoteTableName($table),
                'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ON CLUSTER ' . $this->db->replicatedClusterName,
                $sql
            );
        }

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

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