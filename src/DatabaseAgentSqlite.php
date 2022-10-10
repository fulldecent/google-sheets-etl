<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for putting CSV files into a PDO database
 */
class DatabaseAgentSqlite extends DatabaseAgent
{
    /**
     * Beware max_allowed_packet errors
     * Beware SQLITE_LIMIT_VARIABLE_NUMBER
     *
     * @see https://sqlite.org/c3ref/c_limit_attached.html
     *
     * @var int
     */
    public $sqlInsertChunkSize = 25;

    public const SPREADSHEETS_TABLE = '__meta_spreadsheets';
    public const ETL_JOBS_TABLE = '__meta_etl_jobs';

    // Getters /////////////////////////////////////////////////////////////////

    /**
     * For all spreadsheets which are partially or fully loaded, get the one
     * with the greatest lexical order of (google_modified,
     * google_spreadsheet_id), or null if no spreadsheets are loaded
     *
     * @see https://tools.ietf.org/html/rfc3339
     *
     * @return array like ['2015-01-01 03:04:05', '1-Dcs8ZYoyz82xkjkv3tIbSCAJOOpouXur4dwql4TqiY']
     */
    public function getGreatestModifiedAndIdLoaded(): ?array
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $sql = <<<SQL
SELECT a.google_modified, a.google_spreadsheet_id
  FROM $quotedSpreadsheetsTable a
  JOIN $quotedEtlJobsTable b
    ON b.spreadsheet_id = a.id
 WHERE b.google_modified = a.google_modified
 ORDER BY a.google_modified DESC, a.google_spreadsheet_id DESC
 LIMIT 1
SQL;
        $row = $this->database->query($sql)->fetch(\PDO::FETCH_NUM);
        return $row === false ? null : $row;
    }

    /**
     * For all spreadsheets seen on or after the given date, get the greatest
     * Google spreadsheet ID
     *
     * @param integer $since a Unix timestamp
     * @return string|null
     */
    public function getGreatestIdSeenSince(int $since): ?string
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
SELECT google_spreadsheet_id
  FROM $quotedSpreadsheetsTable
 WHERE last_seen >= ?
 ORDER BY google_spreadsheet_id DESC
 LIMIT 1
SQL;
        $query = $this->database->prepare($sql);
        $query->execute([$since]);
        $result = $query->fetchColumn();
        return $result === false ? null : $result;
    }

    /**
     * Get all spreadsheets which were not seen after the given time
     *
     * @param integer $since  a Unix timestamp
     * @param integer $limit  limit a maximum quantity of results to return
     * @return array          Google spreadsheet IDs in order starting with the
     *                        least and including up to LIMIT number of rows
     */
    public function getIdsNotSeenSince(int $since, int $limit): array
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
SELECT google_spreadsheet_id
  FROM $quotedSpreadsheetsTable
 WHERE last_seen < ?
 ORDER BY google_spreadsheet_id
 LIMIT $limit
SQL;
        $query = $this->database->prepare($sql);
        $query->execute([$since]);
        return $query->fetchColumn();
    }

    /**
     * Get ETL details for a specific spreadsheet and sheet
     *
     * @param string $googleSpreadsheetId  specified Google spreadsheet ID
     * @param string $sheetName            specified sheet name
     * @return \stdClass|null              ETL information
     */
    public function getEtl(string $googleSpreadsheetId, string $sheetName): ?\stdClass {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);

        $getAccountingSql = <<<SQL
SELECT etl_jobs.id
     , etl_jobs.sheet_name
     , etl_jobs.target_table
     , etl_jobs.google_modified loaded_google_modified
     , spreadsheets.google_spreadsheet_id
     , spreadsheets.google_modified latest_google_modified
     , spreadsheets.last_seen
  FROM $quotedEtlJobsTable etl_jobs
  JOIN $quotedSpreadsheetsTable spreadsheets
    ON spreadsheets.id = etl_jobs.spreadsheet_id
 WHERE google_spreadsheet_id = :google_spreadsheet_id
   AND sheet_name = :sheet_name
SQL;
        $statement = $this->database->prepare($getAccountingSql);
        $statement->execute([
            'google_spreadsheet_id' => $googleSpreadsheetId,
            'sheet_name' => $sheetName,
        ]);
        $retval = $statement->fetch(\PDO::FETCH_OBJ);
        return $retval === false ? null : $retval;
    }

    // Accounting //////////////////////////////////////////////////////////////

    /**
     * The accounting must be set up before any other methods are called
     *
     * @apiSpec Calling this method twice shall not cause data loss or error.
     */
    public function setUpAccounting()
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);

        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedSpreadsheetsTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    google_spreadsheet_id VARCHAR(44) NOT NULL,
    google_modified VARCHAR(99) NOT NULL,
    google_spreadsheet_name VARCHAR(100) NOT NULL,
    last_seen INT NOT NULL,
    CONSTRAINT google_spreadsheet_id UNIQUE (google_spreadsheet_id)
);
SQL;
        $this->database->exec($sql);

        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedEtlJobsTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    spreadsheet_id INT NOT NULL,
    sheet_name VARCHAR(99) NOT NULL,
    target_table VARCHAR(99),
    google_modified VARCHAR(99) NOT NULL,
    CONSTRAINT sheet_name UNIQUE (spreadsheet_id, sheet_name),
    FOREIGN KEY (spreadsheet_id)
        REFERENCES $quotedSpreadsheetsTable(id)
);
SQL;
        $this->database->exec($sql);
    }

    /**
     * Account that a spreadsheet is seen, this confirms we have access
     */
    public function accountSpreadsheetSeen(string $googleSpreadsheetId, string $googleModified, string $name)
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
INSERT OR IGNORE INTO $quotedSpreadsheetsTable
(google_spreadsheet_id, google_modified, google_spreadsheet_name, last_seen)
VALUES
(:google_spreadsheet_id, :google_modified, :google_spreadsheet_name, :last_seen)
SQL;
        $this->database->prepare($sql)->execute([
            'google_spreadsheet_id'=>$googleSpreadsheetId,
            'google_modified'=>$googleModified,
            'google_spreadsheet_name'=>$name,
            'last_seen'=>$this->loadTime
        ]);
        $sql = <<<SQL
UPDATE $quotedSpreadsheetsTable
   SET google_modified = :google_modified
     , google_spreadsheet_name = :google_spreadsheet_name
     , last_seen = :last_seen
 WHERE google_spreadsheet_id = :google_spreadsheet_id
SQL;
        $this->database->prepare($sql)->execute([
            'google_spreadsheet_id'=>$googleSpreadsheetId,
            'google_modified'=>$googleModified,
            'google_spreadsheet_name'=>$name,
            'last_seen'=>$this->loadTime
        ]);
    }

    // Data store //////////////////////////////////////////////////////////////

    /**
     * Removes sheet and accounting, if exists, and load and account for sheet
     *
     * @implNote: This could reduce the transaction locking time by using a
     *            temporary table to stage incoming data.
     * @apiSpec This operation shall be atomic, no partial effect may occur on
     *          the database if program is prematurely exited.
     */
    public function loadAndAccountSheet(string $googleSpreadsheetId, string $sheetName, string $targetTable, string $googleModified, array $columnNames, array $rows)
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);

        // Create table ////////////////////////////////////////////////////////
        echo '    Creating table';
        $quotedTargetTable = $this->quotedFullyQualifiedTableName($targetTable);
        $normalizedQuotedColumnNames = $this->normalizedQuotedColumnNames($columnNames);
        $createTableSql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedTargetTable (
    _rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    _origin_etl_job_id INT NOT NULL,
    _origin_row INT NOT NULL,
    CONSTRAINT _origin_row UNIQUE (_origin_etl_job_id, _origin_row),
    FOREIGN KEY (_origin_etl_job_id)
        REFERENCES $quotedEtlJobsTable(id)
        ON DELETE RESTRICT
        ON UPDATE RESTRICT
);
SQL;
        $this->database->exec($createTableSql);
        foreach ($normalizedQuotedColumnNames as $normalizedQuotedColumnName) {
            $addColumnSql = "ALTER TABLE $quotedTargetTable ADD COLUMN $normalizedQuotedColumnName VARCHAR(100)";
            try {
                $this->database->exec($addColumnSql);
            } catch (\PDOException $e) {
                // Ignore if column already exists
            }
        }

        $this->database->beginTransaction(); // the CREATE/UPDATE above are implicit-commit statements

        // Update accounting ///////////////////////////////////////////////////
        $insertAccountingSql = <<<SQL
INSERT OR IGNORE INTO $quotedEtlJobsTable (
           spreadsheet_id,
           sheet_name,
           target_table,
           google_modified
       )
SELECT id, :sheet_name, :target_table, :google_modified
  FROM $quotedSpreadsheetsTable
 WHERE google_spreadsheet_id = :google_spreadsheet_id
SQL;
        $this->database->prepare($insertAccountingSql)->execute([
            'google_spreadsheet_id' => $googleSpreadsheetId,
            'sheet_name' => $sheetName,
            'target_table' => $targetTable,
            'google_modified' => $googleModified
        ]);
        $updateAccountingSql = <<<SQL
UPDATE $quotedEtlJobsTable
   SET target_table = :target_table
     , google_modified = :google_modified
 WHERE spreadsheet_id = (SELECT id FROM $quotedSpreadsheetsTable WHERE google_spreadsheet_id = :google_spreadsheet_id)
   AND sheet_name = :sheet_name
SQL;
        $this->database->prepare($updateAccountingSql)->execute([
            'google_spreadsheet_id' => $googleSpreadsheetId,
            'sheet_name' => $sheetName,
            'target_table' => $targetTable,
            'google_modified' => $googleModified
        ]);
        $getAccountingSql = <<<SQL
SELECT etl_jobs.id
  FROM $quotedEtlJobsTable etl_jobs
  JOIN $quotedSpreadsheetsTable spreadsheets
    ON spreadsheets.id = etl_jobs.spreadsheet_id
 WHERE google_spreadsheet_id = :google_spreadsheet_id
   AND sheet_name = :sheet_name
   AND target_table = :target_table
SQL;
        $statement = $this->database->prepare($getAccountingSql);
        $statement->execute([
            'google_spreadsheet_id' => $googleSpreadsheetId,
            'sheet_name' => $sheetName,
            'target_table' => $targetTable
        ]);
        $etlJobId = $statement->fetchColumn();
        assert($etlJobId !== false);

        // Delete rows /////////////////////////////////////////////////////////
        echo '    Deleting rows';
        $deleteSql = <<<SQL
DELETE FROM $quotedTargetTable
 WHERE _origin_etl_job_id = ?
SQL;
        $this->database->prepare($deleteSql)->execute([$etlJobId]);

        // Insert rows /////////////////////////////////////////////////////////
        echo '    Inserting rows';
        $quotedColumns = implode(',', array_merge(['_origin_etl_job_id', '_origin_row'], $normalizedQuotedColumnNames));
        $sqlPrefix = "INSERT INTO $quotedTargetTable ($quotedColumns) VALUES";
        $sqlOneValueList = implode(',', array_fill(0, count($columnNames) + 2, '?'));
        // Load each row for the selected columns
        foreach (array_chunk($rows, $this->sqlInsertChunkSize, true) as $rowChunk) {
            $parameters = [];
            foreach ($rowChunk as $i => $row) {
                array_push($parameters, $etlJobId, $i, ...$row);
            }
            $sqlValueLists = '(' . implode('),(', array_fill(0, count($rowChunk), $sqlOneValueList)) . ')';
            $statement = $this->database->prepare($sqlPrefix . $sqlValueLists);
            $parameters = array_map(function($v){
                return is_null($v)
                    ? null
                    : is_string($v)
                        ? substr($v, 0, 100)
                        : $v;
            }, $parameters);
            $statement->execute($parameters);
            echo '        loaded ' . (array_key_last($rowChunk) + 1) . ' rows' . PHP_EOL;
        }

        // All done
        $this->database->commit();
    }

    // PRIVATE /////////////////////////////////////////////////////////////////

    /**
     * Use SCHEMA and PREFIX to generate table name.
     *
     * @implNote WARNING, this can make names that are too long for
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

    /**
     * Turns the columns into unique names of the format which MySQL and SQLite
     * allow as ASCII quoted identifiers
     *
     * @implNote: This will break if a column is named _rowid_
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
            array_push($retval, '`' . $column . '`');
        }
        return $retval;
    }
}