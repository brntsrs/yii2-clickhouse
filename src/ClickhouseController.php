<?php
namespace brntsrs\ClickHouse;

use yii\web\Controller;

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

    private function getDbName($connectionString) {
        $matches = array();
        preg_match("/dbname=([^;]*)/", $connectionString, $matches);
        return $matches[1];
    }

    private function getHost($connectionString) {
        $matches = array();
        preg_match("/host=([^;]*)/", $connectionString, $matches);
        return $matches[1];
    }

    function getReplaceParams() {
        $fullParams = include \Yii::getAlias('@app/config/console') . '.php';

        $dbParams = $fullParams['components']['db'];
        $dbParams['database'] = $this->getDbName($fullParams['components']['db']['connectionString']);
        $dbParams['host'] = $this->getHost($dbParams['connectionString']);
        $dbParams['port'] = 3306;
        if ($dbParams['host'] == 'localhost') {
            if (!empty($fullParams['components']['clickHouse']['mysqlIp'])) {
                $dbParams['host'] = $fullParams['components']['clickHouse']['mysqlIp'];
            } else {
                $dbParams['host'] = '127.0.0.1';
            }
        }
        $replaceParams = [];
        foreach ($dbParams as $key => $value) {
            $replaceParams['{DB:' . $key . '}'] = $value;
        }

        return $replaceParams;
    }
}