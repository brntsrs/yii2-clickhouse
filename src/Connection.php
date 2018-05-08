<?php
namespace brntsrs\ClickHouse;

use yii\httpclient\Client;

class Connection extends \kak\clickhouse\Connection
{
    public $portWrite = 8123;

    /** @var bool|Client */
    private $_transportWrite = false;

    /** @var bool|Client */
    private $_transport = false;

    private $_isWrite = false;

    /**
     * @return bool|Client
     */
    public function getTransport()
    {
        return $this->_isWrite ? $this->_transportWrite : $this->_transport;
    }

    public function getIsActive()
    {
        return ($this->_isWrite ? $this->_transportWrite : $this->_transport) !== false;
    }

    public function setToRead()
    {
        $this->_isWrite = false;
    }

    public function setToWrite()
    {
        $this->_isWrite = true;
        if (!$this->isActive) {
            $this->open();
        }
    }

    public function open()
    {
        if ($this->getIsActive()) {
            return;
        }

        $auth = !empty($this->username) ? $this->username . ':' . $this->password  .'@' : '';
        $scheme = 'http';
        $url =  $scheme. '://' . $auth . $this->dsn. ':' . ($this->_isWrite ? $this->portWrite : $this->port);

        $params = [];
        if (!empty($this->database)) {
            $params['database'] = $this->database;
        }
        if (count($params)) {
            $url.= '?' . http_build_query($params);
        }

        if ($this->_isWrite) {
            $this->_transportWrite = new Client([
                'baseUrl'   => $url,
                'transport' => $this->transportClass,
                'requestConfig' => $this->requestConfig,
            ]);
        } else {
            $this->_transport = new Client([
                'baseUrl'   => $url,
                'transport' => $this->transportClass,
                'requestConfig' => $this->requestConfig,
            ]);
        }
    }
}