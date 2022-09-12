<?php
namespace brntsrs\ClickHouse;

use kak\clickhouse\Query;
use yii\db\ArrayExpression;

class ActiveQuery extends \kak\clickhouse\ActiveQuery
{
    /**
     * {@inheritdoc}
     */
    public function populate($rows)
    {
        if (empty($rows)) {
            return [];
        }

        $models = $this->createModels($rows);
        if (!empty($this->with)) {
            $this->findWith($this->with, $models);
        }

        if (!$this->asArray) {
            foreach ($models as $model) {
                $model->afterFind();
            }
        }

        return parent::populate($models);
    }

    public function count($q = '', $db = null)
    {
        if (
            !$this->distinct
            && empty($this->groupBy)
            && empty($this->having)
            && empty($this->union)
        ) {
            return parent::count($q, $db);
        }
        $command = (new Query())
            ->select(["COUNT($q)"])
            ->from(['c' => $this])
            ->createCommand($db);
        $this->setCommandCache($command);

        return $command->queryScalar();
    }

    public function prepare($builder)
    {
        if ($this->primaryModel === null) {
            return $builder;
        } else {
            // lazy loading of a relation
            $where = $this->where;

            if (is_array($this->via)) {
                // via relation
                /* @var $viaQuery \yii\db\ActiveQuery */
                list($viaName, $viaQuery, $viaCallableUsed) = $this->via;
                if ($viaQuery->multiple) {
                    if ($viaCallableUsed) {
                        $viaModels = $viaQuery->all();
                    } elseif ($this->primaryModel->isRelationPopulated($viaName)) {
                        $viaModels = $this->primaryModel->$viaName;
                    } else {
                        $viaModels = $viaQuery->all();
                        $this->primaryModel->populateRelation($viaName, $viaModels);
                    }
                } else {
                    if ($viaCallableUsed) {
                        $model = $viaQuery->one();
                    } elseif ($this->primaryModel->isRelationPopulated($viaName)) {
                        $model = $this->primaryModel->$viaName;
                    } else {
                        $model = $viaQuery->one();
                        $this->primaryModel->populateRelation($viaName, $model);
                    }
                    $viaModels = $model === null ? [] : [$model];
                }
                $this->filterByModels($viaModels);
            } else {
                $this->filterByModels([$this->primaryModel]);
            }

            $this->where = $where;
        }

        if (!empty($this->on)) {
            $builder->andWhere($this->on);
        }

        return $builder;
    }

    /**
     * @param array $models
     */
    private function filterByModels($models)
    {
        $attributes = array_keys($this->link);

        //$attributes = $this->prefixKeyColumns($attributes);

        $values = [];
        if (count($attributes) === 1) {
            // single key
            $attribute = reset($this->link);
            foreach ($models as $model) {
                $value = isset($model[$attribute]) ? $model[$attribute] : null;
                if ($value !== null) {
                    if (is_array($value)) {
                        $values = array_merge($values, $value);
                    } elseif ($value instanceof ArrayExpression && $value->getDimension() === 1) {
                        $values = array_merge($values, $value->getValue());
                    } else {
                        $values[] = $value;
                    }
                }
            }
            if (empty($values)) {
                $this->emulateExecution();
            }
        } else {
            // composite keys

            // ensure keys of $this->link are prefixed the same way as $attributes
            $prefixedLink = array_combine($attributes, $this->link);
            foreach ($models as $model) {
                $v = [];
                foreach ($prefixedLink as $attribute => $link) {
                    $v[$attribute] = $model[$link];
                }
                $values[] = $v;
                if (empty($v)) {
                    $this->emulateExecution();
                }
            }
        }

        if (!empty($values)) {
            $scalarValues = [];
            $nonScalarValues = [];
            foreach ($values as $value) {
                if (is_scalar($value)) {
                    $scalarValues[] = $value;
                } else {
                    $nonScalarValues[] = $value;
                }
            }

            $scalarValues = array_unique($scalarValues);
            $values = array_merge($scalarValues, $nonScalarValues);
        }

        $this->andWhere(['in', $attributes, $values]);
    }

    /**
     * Creates a new Query object and copies its property values from an existing one.
     * The properties being copies are the ones to be used by query builders.
     * @param Query $from the source query object
     * @return Query the new Query object
     */
    public static function queryCreate($from)
    {
        return new Query([
            'where' => $from->where,
            'limit' => $from->limit,
            'offset' => $from->offset,
            'orderBy' => $from->orderBy,
            'indexBy' => $from->indexBy,
            'select' => $from->select,
            'selectOption' => $from->selectOption,
            'distinct' => $from->distinct,
            'from' => $from->from,
            'groupBy' => $from->groupBy,
            'join' => $from->join,
            'having' => $from->having,
            'union' => $from->union,
            'params' => $from->params,
            'withQueries' => $from->withQueries,
        ]);
    }
}