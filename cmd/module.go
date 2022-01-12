package cmd

import "database/sql"

type DbConfig struct {
    User     string
    Password string
    Host     string
    Port     int
    Database string
    Charset  string
}

type Schema struct {
    CatalogName             string         `gorm:"column:CATALOG_NAME"`
    SchemaName              string         `gorm:"column:SCHEMA_NAME"`
    DefaultCharacterSetName string         `gorm:"column:DEFAULT_CHARACTER_SET_NAME"`
    DefaultCollationName    string         `gorm:"column:DEFAULT_COLLATION_NAME"`
    SqlPath                 sql.NullString `gorm:"column:SQL_PATH"`
}

type Table struct {
    TableCatalog   string         `gorm:"column:TABLE_CATALOG"`
    TableSchema    string         `gorm:"column:TABLE_SCHEMA"`
    TableName      string         `gorm:"column:TABLE_NAME"`
    TableType      string         `gorm:"column:TABLE_TYPE"`
    ENGINE         sql.NullString `gorm:"column:ENGINE"`
    VERSION        sql.NullInt64  `gorm:"column:VERSION"`
    RowFormat      sql.NullString `gorm:"column:ROW_FORMAT"`
    TableRows      sql.NullInt64  `gorm:"column:TABLE_ROWS"`
    AvgRowLength   sql.NullInt64  `gorm:"column:AVG_ROW_LENGTH"`
    DataLength     sql.NullInt64  `gorm:"column:DATA_LENGTH"`
    MaxDataLength  sql.NullInt64  `gorm:"column:MAX_DATA_LENGTH"`
    IndexLength    sql.NullInt64  `gorm:"column:INDEX_LENGTH"`
    DataFree       sql.NullInt64  `gorm:"column:DATA_FREE"`
    AutoIncrement  sql.NullInt64  `gorm:"column:AUTO_INCREMENT"`
    CreateTime     sql.NullTime   `gorm:"column:CREATE_TIME"`
    UpdateTime     sql.NullTime   `gorm:"column:UPDATE_TIME"`
    CheckTime      sql.NullTime   `gorm:"column:CHECK_TIME"`
    TableCollation sql.NullString `gorm:"column:TABLE_COLLATION"`
    CHECKSUM       sql.NullInt64  `gorm:"column:CHECKSUM"`
    CreateOptions  sql.NullString `gorm:"column:CREATE_OPTIONS"`
    TableComment   string         `gorm:"column:TABLE_COMMENT"`
}

type Column struct {
    TableCatalog           string         `gorm:"column:TABLE_CATALOG"`
    TableSchema            string         `gorm:"column:TABLE_SCHEMA"`
    TableName              string         `gorm:"column:TABLE_NAME"`
    ColumnName             string         `gorm:"column:COLUMN_NAME"`
    OrdinalPosition        int            `gorm:"column:ORDINAL_POSITION"`
    ColumnDefault          sql.NullString `gorm:"column:COLUMN_DEFAULT"`
    IsNullable             string         `gorm:"column:IS_NULLABLE"`
    DataType               string         `gorm:"column:DATA_TYPE"`
    CharacterMaximumLength sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
    CharacterOctetLength   sql.NullInt64  `gorm:"column:CHARACTER_OCTET_LENGTH"`
    NumericPrecision       sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
    NumericScale           sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
    DatetimePrecision      sql.NullInt64  `gorm:"column:DATETIME_PRECISION"`
    CharacterSetName       sql.NullString `gorm:"column:CHARACTER_SET_NAME"`
    CollationName          sql.NullString `gorm:"column:COLLATION_NAME"`
    ColumnType             string         `gorm:"column:COLUMN_TYPE"`
    ColumnKey              string         `gorm:"column:COLUMN_KEY"`
    EXTRA                  string         `gorm:"column:EXTRA"`
    PRIVILEGES             string         `gorm:"column:PRIVILEGES"`
    ColumnComment          string         `gorm:"column:COLUMN_COMMENT"`
    GenerationExpression   string         `gorm:"column:GENERATION_EXPRESSION"`
}

type Statistic struct {
    TableCatalog string         `gorm:"column:TABLE_CATALOG"`
    TableSchema  string         `gorm:"column:TABLE_SCHEMA"`
    TableName    string         `gorm:"column:TABLE_NAME"`
    NonUnique    int64          `gorm:"column:NON_UNIQUE"`
    IndexSchema  string         `gorm:"column:INDEX_SCHEMA"`
    IndexName    string         `gorm:"column:INDEX_NAME"`
    SeqInIndex   int            `gorm:"column:SEQ_IN_INDEX"`
    ColumnName   string         `gorm:"column:COLUMN_NAME"`
    COLLATION    sql.NullString `gorm:"column:COLLATION"`
    CARDINALITY  sql.NullInt64  `gorm:"column:CARDINALITY"`
    SubPart      sql.NullInt32  `gorm:"column:SUB_PART"`
    PACKED       sql.NullString `gorm:"column:PACKED"`
    NULLABLE     string         `gorm:"column:NULLABLE"`
    IndexType    string         `gorm:"column:INDEX_TYPE"`
    COMMENT      sql.NullString `gorm:"column:COMMENT"`
    IndexComment string         `gorm:"column:INDEX_COMMENT"`
    IsVisible    sql.NullString `gorm:"column:IS_VISIBLE"`
}

type View struct {
    TableCatalog        string `gorm:"column:TABLE_CATALOG"`
    TableSchema         string `gorm:"column:TABLE_SCHEMA"`
    TableName           string `gorm:"column:TABLE_NAME"`
    ViewDefinition      string `gorm:"column:VIEW_DEFINITION"`
    CheckOption         string `gorm:"column:CHECK_OPTION"`
    IsUpdatable         string `gorm:"column:IS_UPDATABLE"`
    DEFINER             string `gorm:"column:DEFINER"`
    SecurityType        string `gorm:"column:SECURITY_TYPE"`
    CharacterSetClient  string `gorm:"column:CHARACTER_SET_CLIENT"`
    CollationConnection string `gorm:"column:COLLATION_CONNECTION"`
}

type TableConstraints struct {
    ConstraintCatalog string `gorm:"column:CONSTRAINT_CATALOG"`
    ConstraintSchema  string `gorm:"column:CONSTRAINT_SCHEMA"`
    ConstraintName    string `gorm:"column:CONSTRAINT_NAME"`
    TableSchema       string `gorm:"column:TABLE_SCHEMA"`
    TableName         string `gorm:"column:TABLE_NAME"`
    ConstraintType    string `gorm:"column:CONSTRAINT_TYPE"`
}

type ReferentialConstraints struct {
    ConstraintCatalog       string `gorm:"column:CONSTRAINT_CATALOG"`
    ConstraintSchema        string `gorm:"column:CONSTRAINT_SCHEMA"`
    ConstraintName          string `gorm:"column:CONSTRAINT_NAME"`
    UniqueConstraintCatalog string `gorm:"column:UNIQUE_CONSTRAINT_CATALOG"`
    UniqueConstraintSchema  string `gorm:"column:UNIQUE_CONSTRAINT_SCHEMA"`
    UniqueConstraintName    string `gorm:"column:UNIQUE_CONSTRAINT_NAME"`
    MatchOption             string `gorm:"column:MATCH_OPTION"`
    UpdateRule              string `gorm:"column:UPDATE_RULE"`
    DeleteRule              string `gorm:"column:DELETE_RULE"`
    TableName               string `gorm:"column:TABLE_NAME"`
    ReferencedTableName     string `gorm:"column:REFERENCED_TABLE_NAME"`
}

type KeyColumnUsage struct {
    ConstraintCatalog          string `gorm:"column:CONSTRAINT_CATALOG"`
    ConstraintSchema           string `gorm:"column:CONSTRAINT_SCHEMA"`
    ConstraintName             string `gorm:"column:CONSTRAINT_NAME"`
    TableCatalog               string `gorm:"column:TABLE_CATALOG"`
    TableSchema                string `gorm:"column:TABLE_SCHEMA"`
    TableName                  string `gorm:"column:TABLE_NAME"`
    ColumnName                 string `gorm:"column:COLUMN_NAME"`
    OrdinalPosition            int64  `gorm:"column:ORDINAL_POSITION"`
    PositionInUniqueConstraint int64  `gorm:"column:POSITION_IN_UNIQUE_CONSTRAINT"`
    ReferencedTableSchema      string `gorm:"column:REFERENCED_TABLE_SCHEMA"`
    ReferencedTableName        string `gorm:"column:REFERENCED_TABLE_NAME"`
    ReferencedColumnName       string `gorm:"column:REFERENCED_COLUMN_NAME"`
}
