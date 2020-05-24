<?php
namespace hyzhak\db\dblib;

use yii\db\ColumnSchema;
use yii\base\NotSupportedException;

class Schema extends \yii\db\Schema
{

    const TYPE_VARCHAR = 'varchar';

    private $_version, $_schema;

    /**
     * @var array the abstract column types mapped to physical column types.
     * @since 1.1.6
     */
    /**
     * @var array mapping from physical column types (keys) to abstract column types (values)
     */
    public $typeMap = [
        // exact numbers
        'bigint' => self::TYPE_BIGINT,
        'numeric' => self::TYPE_DECIMAL,
        'bit' => self::TYPE_SMALLINT,
        'smallint' => self::TYPE_SMALLINT,
        'decimal' => self::TYPE_DECIMAL,
        'integer' => self::TYPE_INTEGER,
        'int' => self::TYPE_INTEGER,
        'tinyint' => self::TYPE_SMALLINT,
        // approximate numbers
        'float' => self::TYPE_FLOAT,
        'double' => self::TYPE_DOUBLE,
        'real' => self::TYPE_FLOAT,
        // date and time
        'date' => self::TYPE_DATE,
        'datetime' => self::TYPE_DATETIME,
        'timestamp' => self::TYPE_TIMESTAMP,
        'time' => self::TYPE_TIME,
        // character strings
        'char' => self::TYPE_CHAR,
        'varchar' => self::TYPE_VARCHAR,
        'nvarchar' => self::TYPE_STRING,
        'text' => self::TYPE_TEXT,
        // binary strings
        'binary' => self::TYPE_BINARY,
    ];

	/**
     * Quotes a table name for use in a query.
     * A simple table name has no schema prefix.
     * @param string $name table name.
     * @return string the properly quoted table name.
     */
    public function quoteSimpleTableName($name)
    {
        return strpos($name, '[') === false ? "[{$name}]" : $name;
    }

    /**
     * Quotes a column name for use in a query.
     * A simple column name has no prefix.
     * @param string $name column name.
     * @return string the properly quoted column name.
     */
    public function quoteSimpleColumnName($name)
    {
        return strpos($name, '[') === false && $name !== '*' ? "[{$name}]" : $name;
    }

    /**
     * Creates a query builder for the MSSQL database.
     * @return QueryBuilder query builder interface.
     */
    public function createQueryBuilder()
    {
        return new QueryBuilder($this->db);
    }

	/**
     * Loads the metadata for the specified table.
     * @param string $name table name
     * @return TableSchema|null driver dependent table metadata. Null if the table does not exist.
     */
    public function loadTableSchema($name)
    {
        if ( $this->getNumberTables($name) > 1)
        throw new NotSupportedException('There are more then one table with this name, please specify owner of the table.');

        $table = new TableSchema();
        //$table->driver='dblib';
        $this->resolveTableNames($table, $name);
        $this->findPrimaryKeys($table);
        if ($this->findColumns($table)) {
            $this->findForeignKeys($table);
            return $table;
        } else {
            return null;
        }
    }

    /**
     * Resolves the table name and schema name (if any).
     * @param TableSchema $table the table metadata object
     * @param string $name the table name
     */
    protected function resolveTableNames($table, $name)
    {
        $parts = explode('.', str_replace(['[', ']'], '', $name));
        $partCount = count($parts);
/* // no catalog in ASA
        if ($partCount === 3) {
            // catalog name, schema name and table name passed
            $table->catalogName = $parts[0];
            $table->schemaName = $parts[1];
            $table->name = $parts[2];
            $table->fullName = $table->catalogName . '.' . $table->schemaName . '.' . $table->name;
        } else
*/
        if ($partCount === 2) {
            // only schema name and table name passed
            $table->schemaName = $parts[0];
            $table->name = $parts[1];
            //$table->rawName=$this->quoteTableName($table->schemaName).'.'.$this->quoteTableName($table->name);
            $table->fullName = $table->schemaName !== $this->defaultSchemaName ? $table->schemaName . '.' . $table->name : $table->name;
        } else {
            // only table name passed
            $table->schemaName = $this->defaultSchemaName;
            $table->name = $parts[0];
            //$table->rawName=$this->quoteTableName($table->schemaName).'.'.$this->quoteTableName($table->name);
            $table->fullName = $table->name = $parts[0];
        }
    }

    /**
     * Collects the primary key column details for the given table.
     * @param TableSchema $table the table metadata
     */
    protected function findPrimaryKeys($table)
    {
        $result = [];
        switch($this->AsaVersion) {
            case 9:
                $sql = <<<SQL
SELECT
    trim(C.column_name) AS field_name
FROM SYS.SYSTABLE T
LEFT OUTER JOIN SYS.SYSCOLUMN C ON
    T.table_id=C.table_id
JOIN SYS.SYSUSERPERMS U ON
    U.user_id = T.creator
WHERE
    upper(table_name)  = upper(:tableName) AND
    upper(U.user_name) = upper(:schemaName) AND
    C.pkey='Y'
SQL;
                break;
            case 11:
            case 12:
            case 16:
            case 17:
                $sql = <<<SQL
SELECT
    trim(c.column_name) field_name
FROM SYS.SYSTABLE t
LEFT OUTER JOIN SYS.SYSUSER U ON
    U.user_id = T.creator
LEFT OUTER JOIN SYS.SYSCOLUMN c ON
    t.table_id=c.table_id
LEFT OUTER JOIN SYS.SYSIDXCOL i ON
    t.table_id=i.table_id AND
    i.index_id = 0 AND
    c.column_id=i.column_id
WHERE
    upper(t.table_name) = upper(:tableName) AND
    upper(U.user_name)  = upper(:schemaName) AND
    c.pkey='Y'
SQL;
                break;
            default:
                throw new NotSupportedException('This version ASA( Ver.'.$this->AsaVersion.') is not supported.');
        }

        $primary = $this->db
            ->createCommand($sql, [
                ':tableName' => $table->name,
                ':schemaName' => $table->schemaName,
            ])
            ->queryAll();

        $result = [];
        foreach ($primary as $row) {
            $result[] = $row['field_name'];
        }
        $table->primaryKey = $result;
	}

    /**
     * Collects the metadata of table columns.
     * @param TableSchema $table the table metadata
     * @return boolean whether the table exists in the database
     */
    protected function findColumns($table)
    {
        switch($this->AsaVersion) {
            case 9:
                $sql = <<<SQL
SELECT DISTINCT
    C.column_id, trim(C.column_name) as column_name, trim(D.domain_name) as base_type, C.width, C.scale, Y.type_name,
    C.[nulls], IFNULL( I.index_id, 'N', 'Y' ) AS [unique], C.pkey as IsPkey, C.column_type, C.[default], NULL, C.remarks
FROM SYS.SYSCOLUMN C
JOIN SYS.SYSDOMAIN D ON
    D.domain_id = C.domain_id
LEFT OUTER JOIN SYS.SYSUSERTYPE Y ON
    Y.type_id = C.user_type
LEFT OUTER JOIN SYS.SYSINDEX I ON
    I.table_id = C.table_id AND I.[unique] = 'U' AND
    (SELECT COUNT(*) FROM SYS.SYSIXCOL XA WHERE XA.table_id = I.table_id AND XA.index_id = I.index_id AND XA.column_id = C.column_id) = 1 AND
    (SELECT COUNT(*) FROM SYS.SYSIXCOL XB WHERE XB.table_id = I.table_id AND XB.index_id = I.index_id AND XB.column_id <> C.column_id) = 0
JOIN SYS.SYSTABLE T ON
    T.table_id = C.table_id
JOIN SYS.SYSUSERPERMS U ON
    U.user_id = T.creator
WHERE
    upper(U.user_name) = upper(:tableName) AND
    upper(T.table_name) = upper(:schemaName)
ORDER BY C.column_id
SQL;
                break;
            case 11:
                $sql = <<<SQL
SELECT DISTINCT
    C.column_id, C.object_id, trim(C.column_name) as column_name, trim(D.domain_name) as base_type, C.width, C.scale, Y.type_name,
    C.[nulls], IFNULL( IU.index_id, 'N', 'Y' ) AS [unique], IFNULL( XP.index_id, 'N', 'Y' ) AS IsPkey, C.column_type, C.[default],
    C.[compressed], C.lob_index, R.remarks
FROM SYS.SYSTABCOL C
JOIN SYS.SYSDOMAIN D ON
    D.domain_id = C.domain_id
LEFT OUTER JOIN SYS.SYSUSERTYPE Y ON
    Y.type_id = C.user_type
LEFT OUTER JOIN SYS.SYSIDX IU ON
    IU.table_id = C.table_id AND IU.index_category = 3 AND
    IU.[unique] = 2  AND
    (SELECT COUNT(*) FROM SYS.SYSIDXCOL XA WHERE XA.table_id = IU.table_id AND XA.index_id = IU.index_id AND XA.column_id = C.column_id) = 1 AND
    (SELECT COUNT(*) FROM SYS.SYSIDXCOL XB WHERE XB.table_id = IU.table_id AND XB.index_id = IU.index_id AND XB.column_id <> C.column_id) = 0
LEFT OUTER JOIN SYS.SYSIDXCOL XP ON
    XP.table_id = C.table_id AND
    XP.column_id = C.column_id AND
    XP.index_id = 0
LEFT OUTER JOIN SYS.SYSREMARK R ON
    R.object_id = C.object_id
JOIN SYS.SYSTAB T ON
    T.table_id = C.table_id
JOIN SYS.SYSUSER U ON
    U.user_id = T.creator
WHERE
    upper(U.user_name) = upper(:tableName) AND
    upper(T.table_name) = upper(:schemaName)
ORDER BY C.column_id
SQL;
                break;
            case 12:
            case 16:
            case 17:
                $sql = <<<SQL
SELECT DISTINCT
    C.column_id, C.object_id, trim(C.column_name) as column_name, trim(D.domain_name) as base_type, C.width, C.scale, Y.type_name,
    C.[nulls], IFNULL( IU.index_id, 'N', 'Y' ) AS [unique], IFNULL( XP.index_id, 'N', 'Y' ) AS IsPkey, C.column_type, C.[default],
    Q.sequence_name, C.[compressed], C.lob_index --, R.remarks
FROM SYS.SYSTABCOL C
JOIN SYS.SYSDOMAIN D ON
    D.domain_id = C.domain_id
LEFT OUTER JOIN SYS.SYSUSERTYPE Y ON
    Y.type_id = C.user_type
LEFT OUTER JOIN SYS.SYSIDX IU ON
    IU.table_id = C.table_id AND
    IU.index_category = 3 AND
    IU.[unique] = 2  AND
    (SELECT COUNT(*) FROM SYS.SYSIDXCOL XA WHERE XA.table_id = IU.table_id AND XA.index_id = IU.index_id AND XA.column_id = C.column_id) = 1 AND
    (SELECT COUNT(*) FROM SYS.SYSIDXCOL XB WHERE XB.table_id = IU.table_id AND XB.index_id = IU.index_id AND XB.column_id <> C.column_id) = 0
LEFT OUTER JOIN SYS.SYSIDXCOL XP ON
    XP.table_id = C.table_id AND
    XP.column_id = C.column_id AND
    XP.index_id = 0
--LEFT OUTER JOIN SYS.SYSREMARK R ON R.object_id = C.object_id
JOIN SYS.SYSTAB T ON
T.table_id = C.table_id
LEFT OUTER JOIN SYS.SYSSEQUENCE Q ON
    C.[default] LIKE string('%',Q.sequence_name,'%')
JOIN SYS.SYSUSER U ON
    U.user_id = T.creator
WHERE
    upper(U.user_name) = upper(:schemaName) AND
    upper(T.table_name) = upper(:tableName)
ORDER BY C.column_id
SQL;
                break;
            default:
                throw new NotSupportedException('This version ASA( Ver.'.$this->AsaVersion.') is not supported.');
        }

        try {
            $columns = $this->db
                ->createCommand($sql, [
                    ':tableName' => $table->name,
                    ':schemaName' => $table->schemaName,
                ])
                ->queryAll();
            if (empty($columns)) {
                return false;
            }
        } catch (\Exception $e) {
            return false;
        }

        foreach ($columns as $column) {
            $column = $this->loadColumnSchema($column);
            foreach ($table->primaryKey as $primaryKey) {
                if (strcasecmp($column->name, $primaryKey) === 0) {
                    $column->isPrimaryKey = true;
                    break;
                }
            }
            if ($column->isPrimaryKey && $column->autoIncrement) {
                $table->sequenceName = '';
            }
            $table->columns[$column->name] = $column;
        }

        return true;
    }

    /**
     * Loads the column information into a [[ColumnSchema]] object.
     * @param array $info column information
     * @return ColumnSchema the column schema object
     */
    protected function loadColumnSchema($info)
    {

        $column = $this->createColumnSchema();
        $column->name = $info['column_name'];
        $column->allowNull = $info['nulls'] === 'Y';
        $column->dbType = $info['base_type'];
        $column->enumValues = []; // mssql has only vague equivalents to enum
        $column->size = $info['width']; // display size of the column (integer)
        $column->scale = $info['scale']; // display size of the column (integer)
        $column->isPrimaryKey = null; // primary key will be determined in findColumns() method
        $column->autoIncrement = $info['default'] == 'autoincrement';
        $column->unsigned = stripos($column->dbType, 'unsigned') !== false;
        //$column->comment = $info['comment'] === null ? '' : $info['comment'];

        $column->type = self::TYPE_STRING;
        if (preg_match('/^(\w+)(?:\(([^\)]+)\))?/', $column->dbType, $matches)) {
            $type = $matches[1];
            if (isset($this->typeMap[$type])) {
                $column->type = $this->typeMap[$type];
            }

            if (!empty($matches[2])) {
                $values = explode(',', $matches[2]);
                $column->size = $column->precision = (int) $values[0];
                if (isset($values[1])) {
                    $column->scale = (int) $values[1];
                }
                if ($column->size === 1 && ($type === 'tinyint' || $type === 'bit')) {
                    $column->type = 'boolean';
                } elseif ($type === 'bit') {
                    if ($column->size > 32) {
                        $column->type = 'bigint';
                    } elseif ($column->size === 32) {
                        $column->type = 'integer';
                    }
                }
            }
        }

        $column->phpType = $this->getColumnPhpType($column);

        if ($info['default'] === '(NULL)') {
            $info['default'] = null;
        }
        if (!$column->isPrimaryKey && ($column->type !== 'timestamp' || $info['default'] !== 'CURRENT_TIMESTAMP')) {
            $column->defaultValue = $column->phpTypecast($info['default']);
        }

        return $column;
    }

    /**
     * Collects the foreign key column details for the given table.
     * @param TableSchema $table the table metadata
     */
    protected function findForeignKeys($table)
    {
        switch($this->AsaVersion) {
            case 9:
                $sql = <<<SQL
SELECT
    F.foreign_key_id, F.role, trim(PT.table_name) as 'UQ_TABLE_NAME', PU.user_name,
    trim(LIST( FC.column_name, ', ' ORDER BY K.foreign_column_id )) as 'FK_COLUMN_NAME',
    trim(LIST( PC.column_name, ', ' ORDER BY K.foreign_column_id )) as 'UQ_COLUMN_NAME',
    F.check_on_commit, F.[nulls], UT.referential_action, DT.referential_action,
    IFNULL( A.attribute_value, 'N', 'Y' ) AS [clustered], F.hash_limit, F.remarks
FROM SYS.SYSFOREIGNKEY F
JOIN SYS.SYSFKCOL K ON
    K.foreign_table_id = F.foreign_table_id AND
    K.foreign_key_id = F.foreign_key_id
JOIN SYS.SYSCOLUMN FC ON
    FC.table_id = F.foreign_table_id AND
    FC.column_id = K.foreign_column_id
JOIN SYS.SYSCOLUMN PC ON
    PC.table_id = F.primary_table_id AND
    PC.column_id = K.primary_column_id
JOIN SYS.SYSTABLE FT ON
    FT.table_id = F.foreign_table_id
JOIN SYS.SYSUSERPERMS FU ON
    FU.user_id = FT.creator
LEFT OUTER JOIN SYS.SYSATTRIBUTE A ON
    A.object_type = 'T' AND
    A.object_id = FT.table_id AND
    A.attribute_id = 2 AND
    A.attribute_value = F.foreign_key_id
JOIN SYS.SYSTABLE PT ON
    PT.table_id = F.primary_table_id
JOIN SYS.SYSUSERPERMS PU ON
    PU.user_id = PT.creator
LEFT OUTER JOIN SYS.SYSTRIGGER UT ON
    UT.foreign_table_id = F.foreign_table_id AND
    UT.foreign_key_id = F.foreign_key_id AND
    UT.[event] = 'C'
LEFT OUTER JOIN SYS.SYSTRIGGER DT ON
    DT.foreign_table_id = F.foreign_table_id AND
    DT.foreign_key_id = F.foreign_key_id AND
    DT.[event] = 'D'
WHERE
    upper(FU.user_name)  = upper(:schemaName) AND
    upper(FT.table_name) = upper(:tableName)
GROUP BY F.foreign_key_id, F.role, PT.table_name, PU.user_name, F.check_on_commit, F.[nulls], UT.referential_action, DT.referential_action, [clustered], F.hash_limit, F.remarks
ORDER BY F.role
SQL;
                break;
            case 11:
            case 12:
            case 16:
            case 17:
                $sql = <<<SQL
SELECT
    trim(C.column_name) as 'FK_COLUMN_NAME', trim(T.table_name) as 'UQ_TABLE_NAME', trim(PC.column_name) as 'UQ_COLUMN_NAME'
FROM SYS.SYSIDX I
JOIN SYS.SYSTABLE T ON
    T.table_id = I.table_id
JOIN SYS.SYSUSER U ON
    U.user_id = T.creator
JOIN SYS.SYSIDXCOL X ON
    X.table_id = I.table_id AND
    X.index_id = I.index_id
JOIN SYS.SYSTABCOL C ON
    C.table_id = X.table_id AND
    C.column_id = X.column_id
LEFT OUTER JOIN (
    SYS.SYSFKEY F JOIN SYS.SYSIDX PI ON
        PI.table_id = F.primary_table_id AND
        PI.index_id = F.primary_index_id
    JOIN SYS.SYSTABLE PT ON
        PT.table_id = PI.table_id
    JOIN SYS.SYSTABCOL PC ON
        PC.table_id = F.primary_table_id
    ) ON
    F.foreign_table_id = I.table_id AND
    F.foreign_index_id = I.index_id AND
    PC.column_id = X.primary_column_id
WHERE I.index_category IN ( 2 ) AND
    upper(U.user_name)  = upper(:schemaName) AND
    upper(T.table_name) = upper(:tableName)
ORDER BY PT.table_name
SQL;
                break;
            default:
                throw new NotSupportedException('This version ASA( Ver.'.$this->AsaVersion.') is not supported.');
        }


        $rows = $this->db->createCommand($sql, [
            ':tableName' => $table->name,
            ':schemaName' => $table->schemaName,
        ])->queryAll();
        $table->foreignKeys = [];
        foreach ($rows as $row) {
            $table->foreignKeys[] = [$row['UQ_TABLE_NAME'], $row['FK_COLUMN_NAME'] => $row['UQ_COLUMN_NAME']];
        }
    }

    /**
     * Returns all table names in the database.
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
	 * @param boolean $includeViews whether to include views in the result. Defaults to true.
     * @return array all table names in the database. The names have NO schema name prefix.
     */
    protected function findTableNames($schema = '', $includeViews=true)
    {
        if ($schema === '') {
            $schema = $this->defaultSchemaName;
        }
        $condition = ($includeViews) ? "TABLE_TYPE in ('BASE','VIEW')" : "TABLE_TYPE ='BASE'";

        switch($this->AsaVersion) {
            case 9:
                $table_shift ="sys.sysuserperms";
                break;
            case 11:
            case 12:
            case 16:
            case 17:
                $table_shift ="sys.sysuser";
                break;
            default:
                throw new NotSupportedException('This version ASA( Ver.'.$this->AsaVersion.') is not supported.');
        }

        $sql = <<<SQL
SELECT trim(table_name) as table_name, trim(user_name) as TABLE_SCHEMA
FROM sys.systable
LEFT OUTER JOIN {$table_shift} ON
    creator=user_id
WHERE
    UPPER(user_name)=UPPER(:schema) AND
    creator <> 0 AND
    user_name NOT IN('rs_systabgroup') AND
    {$condition}
SQL;

        return $this->db->createCommand($sql, [':schema' => $schema])->queryColumn();
    }

	/**
     * Compares two table names.
     * The table names can be either quoted or unquoted. This method
     * will consider both cases.
     * @param string $name1 table name 1
     * @param string $name2 table name 2
     * @return boolean whether the two table names refer to the same table.
     */
    public function compareTableNames($name1,$name2)
	{
        $name1=str_replace(array('[',']'),'',$name1);
        $name2=str_replace(array('[',']'),'',$name2);
        return parent::compareTableNames(strtolower($name1),strtolower($name2));
    }

    /**
     * Resets the sequence value of a table's primary key.
     * The sequence will be reset such that the primary key of the next new row inserted
     * will have the specified value or 1.
     * @param CDbTableSchema $table the table schema whose primary key sequence will be reset
     * @param mixed $value the value for the primary key of the next new row inserted. If this is not set,
     * the next new row's primary key will have a value 1.
     * @since 1.1.6
     */
    public function resetSequence($table,$value=null)
    {
        if($table->sequenceName!==null)
        {
            $db=$this->getDbConnection();
            if($value===null)
                $value=$db->createCommand("SELECT MAX(`{$table->primaryKey}`) FROM {$table->rawName}")->queryScalar();

            $value=(int)$value;
            //pk
            $db->createCommand("CALL sa_reset_identity ('$table->name', NULL, $value)")->execute();
            //real seq
            //$db->createCommand("alter sequence SEQ-NAME restart with $value")->execute();
        }
    }

    private $_normalTables=array();  // non-view tables

    /**
     * Enables or disables integrity check.
     * @param boolean $check whether to turn on or off the integrity check.
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     * @since 1.1.6
     */
    public function checkIntegrity($check=true,$schema='')
    {
        //asa has no this option
        //use LOAD TABLE with  CHECK CONSTRAINTS { ON | OFF }
        //or set the server in bulk mode with option -b
        return;
    }

    /**
     * Creates a table column.
     * @param array $column column metadata
     * @return CDbColumnSchema normalized column metadata
     */
    protected function createColumn($column)
    {
        $c=new CMdblibColumnSchema;
        $c->name=$column['column_name'];
        $c->rawName=$this->quoteColumnName($c->name);
        $c->allowNull=$column['nulls']=='Y';
        if ($column['scale']!==0)
        {
            // We have a numeric datatype
            $c->size=$c->precision=$column['width'];
            $c->scale=$column['scale'];
        }
        elseif ($column['base_type']=='image' || $column['base_type']=='text')
            $c->size=$c->precision=null;
        else
            $c->size=$c->precision=$column['width'];

       switch($this->AsaVersion)
        {
            case 9:
            case 11:
                $c->init($column['base_type'], strtoupper($column['default']) == strtoupper('autoincrement')
                    || strtoupper($column['default']) == strtoupper('global autoincrement')
                    ? null
                    : $column['default']);
                break;
            case 12:
            case 16:
            case 17:
                $c->init($column['base_type'],strtoupper($column['default']) == strtoupper('autoincrement')
                    || strtoupper($column['default'])==strtoupper('global autoincrement')
                    || !is_null($column['sequence_name']
                )
                ? null
                : $column['default']);
                break;

                default:
                    throw new Exception("This version ASA( Ver.$this->AsaVersion ) is not supported.");
        }

        return $c;
    }

    /**
     * Creates a command builder for the database.
     * This method overrides parent implementation in order to create a Mdblib specific command builder
     * @return CDbCommandBuilder command builder instance
     */
    protected function createCommandBuilder()
    {
        return new CMdblibCommandBuilder($this);
    }

    /**
     * Builds a SQL statement for renaming a DB table.
     * @param string $table the table to be renamed. The name will be properly quoted by the method.
     * @param string $newName the new table name. The name will be properly quoted by the method.
     * @return string the SQL statement for renaming a DB table.
     * @since 1.1.6
     */
    public function renameTable($table, $newName)
    {
        return "ALTER TABLE $table rename $newName";
    }

    /**
     * Builds a SQL statement for renaming a column.
     * @param string $table the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $name the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     * @return string the SQL statement for renaming a DB column.
     * @since 1.1.6
     */
    public function renameColumn($table, $name, $newName)
    {
        return "ALTER TABLE $table RENAME $name TO $newName";
    }

    /**
     * Builds a SQL statement for changing the definition of a column.
     * @param string $table the table whose column is to be changed. The table name will be properly quoted by the method.
     * @param string $column the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $type the new column type. The {@link getColumnType} method will be invoked to convert abstract column type (if any)
     * into the physical one. Anything that is not recognized as abstract type will be kept in the generated SQL.
     * For example, 'string' will be turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not null'.
     * @return string the SQL statement for changing the definition of a column.
     * @since 1.1.6
     */
    public function alterColumn($table, $column, $type)
    {
        $type=$this->getColumnType($type);
        $sql='ALTER TABLE ' . $this->quoteTableName($table) . ' MOFIFY '
            . $this->quoteColumnName($column) . ' '
            . $this->getColumnType($type);
        return $sql;
    }

    public function getAsaVersion()
    {
        if (empty($this->_version)) {
            $res = $this->db->createCommand('SELECT @@version as ver')->queryAll();
            $ver = explode(".", $res[0]['ver']);
            $this->_version = $ver[0];
        }
        return $this->_version;
    }

    public function getDefaultSchemaName()
    {
        if(empty($this->_schema)) {
            $this->_schema = $this->db->username ? : 'dbo';
        }
        return $this->_schema;
    }

    /*
     * SQLAnywhere allows to create tables with a same name what belongs to different owners.
     * By default, if user select the table without owner, it supposed, this table belongs to the user.
     * Basivally it loginname.tablename. If access granted for table of differ owner, user should specify
     * owner login as prefix. To avoid a mess this check is added, otherwise $this->findColumns returns columns
     * fro both tables.
     */
    private function getNumberTables($table)
    {
        $sql = 'SELECT COUNT(*) AS cnt FROM SYS.SYSTABLE WHERE UPPER(table_name)=UPPER(:tableName)';
        return $this->db
            ->createCommand($sql, [
                ':tableName' => $table,
            ])
            ->queryScalar();
    }

}
