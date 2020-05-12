<?php
namespace hyzhak\db;

use PDO;

class Connection extends \yii\db\Connection {

    /**
     * @var array mapping between PDO driver names and [[Schema]] classes.
     * The keys of the array are PDO driver names while the values the corresponding
     * schema class name or configuration. Please refer to [[Yii::createObject()]] for
     * details on how to specify a configuration.
     *
     * This property is mainly used by [[getSchema()]] when fetching the database schema information.
     * You normally do not need to set this property unless you want to use your own
     * [[Schema]] class to support DBMS that is not supported by Yii.
     */
    public $schemaMap = [
        'dblib'   => 'hyzhak\db\dblib\Schema', // ASA
    ];

	public $pdoClass = 'hyzhak\db\dblib\PDO';
}
