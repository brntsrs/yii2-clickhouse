<?php
namespace brntsrs\ClickHouse;

use kak\clickhouse\ColumnSchemaBuilder;
use yii\console\Controller;
use yii\db\Expression;

class ClickhouseController extends Controller
{
    public $dictionariesPath = '';

    public function init()
    {
        parent::init();

        if (empty($this->dictionariesPath)) {
            $this->dictionariesPath = \Yii::getAlias('@app/config/dictionaries');
        }
    }

    public function actionDropMigrationsInfo()
    {
        $migrationsList = [];
        $directory = dir(\Yii::getAlias('@app/migrations'));
        while (false !== ($entry = $directory->read())) {
            if (strpos($entry, '.php') !== false) {
                $content = file_get_contents(\Yii::getAlias('@app/migrations') . DIRECTORY_SEPARATOR . $entry);
                if (strpos($content, '$this->db = Yii::$app->clickhouse') !== false || stripos($content, 'MergeTree') !== false) {
                    $migrationsList[] = str_replace('.php', '', $entry);
                }
            }
        }
        $directory->close();

        if (!empty($migrationsList)) {
            \Yii::$app->db->createCommand()->delete('migration', [
                'version' => $migrationsList
            ])->execute();
        }
    }

    public function actionReplicate()
    {
        /**
         * @var Connection $connection
         */
        $connection = \Yii::$app->clickhouse;
        foreach ($connection->schema->tableSchemas as $table) {
            echo $table->name, '... ';

            $ddl = $connection->createCommand('SHOW CREATE TABLE ' . $table->name)->queryOne()['statement'];
            $optionsPosition = strpos($ddl, 'ENGINE = MergeTree()');
            if ($optionsPosition === false) {
                echo 'no MergeTree, skipping', "\r\n";
                continue;
            }
            $options = substr($ddl, $optionsPosition);

            $columns = [];
            $columnNames = [];
            foreach ($table->columns as $columnSchema) {
                $column = new ColumnSchemaBuilder($columnSchema->type, $columnSchema->size);
                if (!empty($columnSchema->defaultValue)) {
                    $column->defaultValue(new Expression($columnSchema->defaultValue));
                }
                if (!empty($columnSchema->unsigned)) {
                    $column->unsigned();
                }
                $columns[$columnSchema->name] = $column;
                $columnNames[] = $columnSchema->name;
            }
            $nonReplicatedTableName = $table->name . '_replicated';

            $connection->isReplicated = false;
            $connection->createCommand()->renameTable($table->name, $nonReplicatedTableName);
            $connection->isReplicated = true;

            $command = new Command();
            $command->db = $connection;
            $command->db->enableSlaves = false;
            $command->createTable($table->name, $columns, $options);
            $command->execute();
            echo 'created... ';

            $columnNames = implode(', ', $columnNames);
            $connection->createCommand(<<<SQL
INSERT INTO {$table->name} ({$columnNames}) SELECT ({$columnNames}) FROM {$nonReplicatedTableName}
SQL )->execute();

            echo 'data moved', "\r\n";
        }
    }

    public function actionDictionaries()
    {
        $dictionaryList = $this->getDictionaries();

        $replaceParams = $this->getReplaceParams();
        echo '<dictionaries>', "\r\n\r\n";
        foreach ($dictionaryList as $dictionary) {
            echo str_replace(array_keys($replaceParams), array_values($replaceParams), file_get_contents($this->dictionariesPath . '/' . $dictionary . '.xml')), "\r\n\r\n";
        }
        echo '</dictionaries>', "\r\n\r\n";

    }

    private function getDictionaries()
    {
        $dictionaryList = [];
        $d = dir($this->dictionariesPath);
        while (false !== ($entry = $d->read())) {
            if (strpos($entry, '.xml') !== false) {
                $dictionaryList[] = str_replace('.xml', '', $entry);
            }
        }
        $d->close();

        return $dictionaryList;
    }

    private function getDbName($dsn) {
        $matches = array();
        preg_match("/dbname=([^;]*)/", $dsn, $matches);
        return $matches[1];
    }

    private function getHost($dsn) {
        $matches = array();
        preg_match("/host=([^;]*)/", $dsn, $matches);
        return $matches[1];
    }

    function getReplaceParams() {
        $fullParams = include \Yii::getAlias('@app/config/console') . '.php';

        $dbParams = $fullParams['components']['db'];
        $dbParams['database'] = $this->getDbName($fullParams['components']['db']['dsn']);
        $dbParams['host'] = $this->getHost($dbParams['dsn']);
        $dbParams['port'] = 3306;
        $dbParams['prefix'] = Dictionary::getDbPrefix();
        if ($dbParams['host'] == 'localhost') {
            if (!empty($fullParams['components']['clickHouse']['mysqlIp'])) {
                $dbParams['host'] = $fullParams['components']['clickHouse']['mysqlIp'];
            } else {
                $dbParams['host'] = '127.0.0.1';
            }
        }
        $replaceParams = [];
        foreach ($dbParams as $key => $value) {
            if (!is_array($value)) {
                $replaceParams['{DB:' . $key . '}'] = $value;
            }
        }

        return $replaceParams;
    }
}