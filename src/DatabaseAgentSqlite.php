<?php
namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for putting CSV files into a PDO database
 */
class DatabaseAgentSqlite extends DatabaseAgent
{
    private /* ?string */ $schema;
    private /* ?string */ $tablePrefix; // Beware of maximum table name length
    const META_TABLE_NAME = '__meta_table_index';
    public $sqlInsertChunkSize = 500; // Beware max_allowed_packet errors

    /**
     * The latest modified time in the database
     *
     * @see https://tools.ietf.org/html/rfc3339
     * 
     * @return ?string The time, or a default value before Google Drive existed
     */
    function getLatestMotidifedTime(): ?string {
    echo 'TODO: refactor this so that it get the last spreadsheet that was fully loaded. As current the way this is used it can skip other sheets on reload if one sheet causes a crash' . PHP_EOL;
        $quotedMetaTableName = $this->quotedFullyQualifiedTableName(self::META_TABLE_NAME);
    $sql = <<<SQL
SELECT MAX(last_modified)
FROM $quotedMetaTableName
WHERE last_modified = last_loaded
SQL;
        return $this->database->query($sql)->fetchColumn();
    }
    
    /**
     * @param string $date YYYY-MM-DD format
     */
    function getGreatestIdWithAuthorizationCheckedSince(string $date): ?string
    {
        $quotedMetaTableName = $this->quotedFullyQualifiedTableName(self::META_TABLE_NAME);
        $sql = <<<SQL
SELECT MAX(spreadsheet_id)
FROM $quotedMetaTableName
WHERE last_authorization_checked >= ?
SQL;
        $statement = $this->database->prepare($sql);
        $statement->execute([$date]);
        return $statement->fetchColumn();
    }
    
    function setupDatabase()
    {
        $quotedMetaTableName = $this->quotedFullyQualifiedTableName(self::META_TABLE_NAME);
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedMetaTableName (
    spreadsheet_id TEXT,
    sheet_name TEXT,
    table_name TEXT,
    last_modified TEXT, -- Use rfc3339
    last_loaded TEXT,
    last_authorization_checked TEXT,
    CONSTRAINT spreadsheet_id UNIQUE (spreadsheet_id,sheet_name)
)
SQL;
        $this->database->exec($sql);
    }

    /// Creates an empty table, replacing if exists
    function createTable(string $unqualifiedTableName, array $columns)
    {
        $quotedTableName = $this->quotedFullyQualifiedTableName($unqualifiedTableName);

        // Drop table
        $dropTableSql = "DROP TABLE IF EXISTS $quotedTableName";
        $this->database->exec($dropTableSql);

        // Create table
        $quotedColumnArray = $this->normalizedQuotedColumnNames($columns);
        $quotedColumns = implode(',', $quotedColumnArray);
        $createTableSql = "CREATE TABLE $quotedTableName ($quotedColumns)";
        echo $createTableSql . PHP_EOL . PHP_EOL . PHP_EOL;
        $this->database->exec($createTableSql);
    }
    

    // Cannot use CSV import with SQLite
    function insertRows(string $unqualifiedTableName, array $rows)
    {
        if (!count($rows)) {
            return;
        }
        $quotedTableName = $this->quotedFullyQualifiedTableName($unqualifiedTableName);


        $sqlPrefix = "INSERT INTO $quotedTableName VALUES";
        $sqlOneValueList = implode(',', array_fill(0, count($rows[0]), '?'));
    
        // Load each row for the usable columns
        foreach(array_chunk($rows, $this->sqlInsertChunkSize, true) as $rowChunk) {
            $parameters = array_merge(...$rowChunk);
            $sqlValueLists = '(' . implode('),(', array_fill(0, count($rowChunk), $sqlOneValueList)) . ')';
            $statement = $this->database->prepare($sqlPrefix . $sqlValueLists);
            $statement->execute($parameters);
            echo "        loaded " . ($this->array_key_last($rowChunk) + 1) . " rows" . PHP_EOL;
        }
    }

    function storeALocation(string $spreadsheetId, string $sheetName, string $modifiedTime, string $unqualifiedTableName)
    {
        $quotedMetaTableName = $this->quotedFullyQualifiedTableName(self::META_TABLE_NAME);
        $accountingSql = <<<SQL
REPLACE INTO $quotedMetaTableName (
    spreadsheet_id, sheet_name, table_name, last_modified, last_loaded, last_authorization_checked
)
VALUES (?, ?, ?, ?, ?, ?)
SQL;
        $statement = $this->database->prepare($accountingSql);
        $statement->execute([$spreadsheetId, $sheetName, $unqualifiedTableName, $modifiedTime, $this->loadTime, $this->loadTime]);
    }

    /**
     * If the Google Sheet is in the database then update the authorization
     * confirmed time to now.
     */
    function storeAuthorizationConfirmation(string $spreadsheetId)
    {
        assert(0);
        //TODO
    }




    /* Private functions ******************************************************/

    /**
     * Turns the columns into unique names of the format which MySQL and SQLite
     * allow as ASCII quoted identifiers
     * 
     * @implNote: This will break if a column is named __rowid
     *
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     * identifiers.
     *
     * @param array $columns
     * @return array The new column names
     */
    private function normalizedQuotedColumnNames(array $columns): array
    {
        $retval = [];
        foreach ($columns as $index => $column) {
            $column = iconv('UTF-8', 'ASCII//TRANSLIT', $column);
            $column = strtolower($column);
            $column = preg_replace('/[^a-z0-9_ ]/', '', $column);
            $column = trim($column);
            if (!preg_match('/^[a-z_]/', $column)) {
                $column = '_' . $column;
            }
            if (preg_match('/^col_[0-9]+$/', $column) || empty($column) || in_array($column, $retval)) {
                $column = 'col_' . ($index + 1);
            }
            $retval[] = '`' . $column . '`';
        }
        return $retval;
    }

    /**
     * Use SCHEMA and PREFIX to generate table name.
     * 
     * @implNote WARNING, this make generate names that are too long for
     *           the database.
     *
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     *
     * @param string $unqualifiedName
     * @return string
     */
    private function quotedFullyQualifiedTableName(string $unqualifiedName): string
    {
        $qualifiedTableName = ($this->tablePrefix ?? '') . $unqualifiedName;
        return ($this->schema ?? '') . "`$qualifiedTableName`";
    }

    protected function array_key_last(array $array)
    {
        end($array);
        return key($array);
    }
}