<?php
namespace brntsrs\ClickHouse;

class ClickhouseId
{
    public static function split($id)
    {
        if (strpos($id, '.') !== false) {
            [$id, $trackTime] = explode('.', $id);
        } else {
            $decodedId = @base64_decode($id . '=');
            if (!empty($decodedId)) {
                return self::split($decodedId);
            }
            $trackTime = null;
        }
        if (!empty($trackTime)) {
            $trackTime = intval($trackTime);
        } else {
            $trackTime = null;
        }

        return [intval($id), $trackTime];
    }
}