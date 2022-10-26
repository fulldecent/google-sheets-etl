<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for spreadsheets in a database
 */
class DatabaseAgentMysql extends DatabaseAgent
{
    /**
     * Beware max_allowed_packet errors
     * @var int
     */
    public int $sqlInsertChunkSize = 500;

    public const SPREADSHEETS_TABLE = '__meta_spreadsheets';
    public const ETL_JOBS_TABLE = '__meta_etl_jobs';

    // Getters /////////////////////////////////////////////////////////////////////////////////////////////////////////

    /// @inheritdoc
    public function getGreatestModified(): ?array
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
SELECT a.google_modified, a.google_spreadsheet_id
  FROM $quotedSpreadsheetsTable a
 ORDER BY a.google_modified DESC, a.google_spreadsheet_id DESC
 LIMIT 1
SQL;
        $row = $this->database->query($sql)->fetch(\PDO::FETCH_NUM);
        return $row === false ? null : $row;
    }

    /// @inheritdoc
    public function getOldestSeen(): ?string
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
SELECT a.google_spreadsheet_id
  FROM $quotedSpreadsheetsTable a
 ORDER BY a.last_seen
 LIMIT 1
SQL;
        $column = $this->database->query($sql)->fetchColumn();
        return $column === false ? null : $column;
    }

    /// @inheritdoc
    public function filterExtractable(array $jobs): array
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $quotedIn = '("NONE", "NONE")'; // sqlite doesn't like empty IN
        $params = [];
        foreach ($jobs as $job) {
            $quotedIn .= ', (?, ?)';
            $params[] = $job->googleSpreadsheetId;
            $params[] = $job->sheetName;
        }

        $sql = <<<SQL
SELECT a.google_spreadsheet_id, b.sheet_name
  FROM $quotedSpreadsheetsTable a
  LEFT JOIN $quotedEtlJobsTable b
    ON b.spreadsheet_id = a.id
 WHERE (a.google_spreadsheet_id, b.sheet_name) IN ($quotedIn)
   AND (a.google_modified = b.google_modified)
SQL;
        $statement = $this->database->prepare($sql);
        $statement->execute($params);
        $rows = $statement->fetchAll(\PDO::FETCH_NUM);
        $alreadyLoadedSpreadsheetIdsAndSheetNames = [];
        foreach ($rows as $row) {
            $alreadyLoadedSpreadsheetIdsAndSheetNames[$row[0]][$row[1]] = true;
        }

        $extractable = [];
        foreach ($jobs as $job) {
            if (!isset($alreadyLoadedSpreadsheetIdsAndSheetNames[$job->googleSpreadsheetId][$job->sheetName])) {
                $extractable[] = $job;
            }
        }
        return $extractable;
    }

    // Setters /////////////////////////////////////////////////////////////////////////////////////////////////////////

    /// @inheritdoc
    public function setUpAccounting(): void
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);

        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedSpreadsheetsTable (
    id INT NOT NULL AUTO_INCREMENT,
    google_spreadsheet_id VARCHAR(44) NOT NULL,
    google_modified VARCHAR(99) NOT NULL,
    google_spreadsheet_name VARCHAR(100) NOT NULL,
    last_seen INT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY google_spreadsheet_id (google_spreadsheet_id)
) ENGINE=InnoDB;
SQL;
        $this->database->exec($sql);

        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedEtlJobsTable (
    id INT NOT NULL AUTO_INCREMENT,
    spreadsheet_id INT NOT NULL,
    sheet_name VARCHAR(99) NOT NULL,
    target_table VARCHAR(99) NOT NULL,
    google_modified VARCHAR(99) NOT NULL,
    raw_columns_rows_hash VARCHAR(99) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY sheet_name (spreadsheet_id, sheet_name),
    FOREIGN KEY (spreadsheet_id)
        REFERENCES $quotedSpreadsheetsTable(id)
        ON DELETE RESTRICT
        ON UPDATE RESTRICT
) ENGINE=InnoDB;
SQL;
        $this->database->exec($sql);
    }

    /// @inheritdoc
    public function setSpreadsheetSeen(string $googleSpreadsheetId, string $googleModified, string $name): void
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
INSERT INTO $quotedSpreadsheetsTable
(google_spreadsheet_id, google_modified, google_spreadsheet_name, last_seen)
VALUES
(:google_spreadsheet_id, :google_modified, :google_spreadsheet_name, :last_seen)
    ON DUPLICATE KEY
UPDATE google_modified = :google_modified,
       google_spreadsheet_name = :google_spreadsheet_name,
       last_seen = :last_seen
SQL;
        $this->database->prepare($sql)->execute([
            'google_spreadsheet_id'=>$googleSpreadsheetId,
            'google_modified'=>$googleModified,
            'google_spreadsheet_name'=>$name,
            'last_seen'=>$this->loadTime,
        ]);
    }

    /// @inheritdoc
    public function createTable(string $targetTable, array $columnNames): void
    {
        $quotedTargetTable = $this->quotedFullyQualifiedTableName($targetTable);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $normalizedQuotedColumnNames = $this->normalizedQuotedColumnNames($columnNames);
        $createTableSql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedTargetTable (
    _rowid INT NOT NULL AUTO_INCREMENT,
    _origin_etl_job_id INT NOT NULL,
    _origin_row INT NOT NULL,
    PRIMARY KEY (_rowid),
    UNIQUE KEY _origin_row (_origin_etl_job_id, _origin_row),
    FOREIGN KEY (_origin_etl_job_id)
        REFERENCES $quotedEtlJobsTable(id)
        ON DELETE RESTRICT
        ON UPDATE RESTRICT
) ENGINE=InnoDB;
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
    }
    
    /// @inheritdoc
    public function loadSheet(
        string $googleSpreadsheetId,
        string $sheetName,
        string $targetTable,
        array $columnNames,
        array $rows,
        string $hash,
    ): void
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedEtlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $quotedTargetTable = $this->quotedFullyQualifiedTableName($targetTable);

        $this->database->beginTransaction(); // the CREATE/UPDATE above are implicit-commit statements

        // Update accounting
        $getHashSql = <<<SQL
SELECT etl_jobs.raw_columns_rows_hash
  FROM $quotedEtlJobsTable etl_jobs
  JOIN $quotedSpreadsheetsTable spreadsheets
    ON spreadsheets.id = etl_jobs.spreadsheet_id
 WHERE google_spreadsheet_id = :google_spreadsheet_id
   AND sheet_name = :sheet_name
SQL;
        $statement = $this->database->prepare($getHashSql);
        $statement->execute([
            'google_spreadsheet_id' => $googleSpreadsheetId,
            'sheet_name' => $sheetName,
        ]);
        $existingHash = $statement->fetchColumn();

        $upsertAccountingSql = <<<SQL
INSERT INTO $quotedEtlJobsTable
(spreadsheet_id, sheet_name, target_table, google_modified, raw_columns_rows_hash)
SELECT spreadsheets.id, :sheet_name, :target_table, google_modified, :raw_columns_rows_hash
  FROM $quotedSpreadsheetsTable spreadsheets
 WHERE google_spreadsheet_id = :google_spreadsheet_id
    ON DUPLICATE KEY
UPDATE target_table = :target_table,
       google_modified = spreadsheets.google_modified,
       raw_columns_rows_hash = :raw_columns_rows_hash
SQL;
        $statement = $this->database->prepare($upsertAccountingSql);
        $statement->execute([
            'google_spreadsheet_id' => $googleSpreadsheetId,
            'sheet_name' => $sheetName,
            'target_table' => $targetTable,
            'raw_columns_rows_hash' => $hash,
        ]);

        if ($existingHash === $hash) {
            echo "    No change in data, skipping load\n";
            $this->database->commit();
            return;
        }

        $getEtlJobIdSql = <<<SQL
SELECT etl_jobs.id
  FROM $quotedEtlJobsTable etl_jobs
  JOIN $quotedSpreadsheetsTable spreadsheets
    ON spreadsheets.id = etl_jobs.spreadsheet_id
 WHERE google_spreadsheet_id = :google_spreadsheet_id
   AND sheet_name = :sheet_name
SQL;
        $statement = $this->database->prepare($getEtlJobIdSql);
        $statement->execute([
            'google_spreadsheet_id' => $googleSpreadsheetId,
            'sheet_name' => $sheetName,
        ]);
        $etlJobId = $statement->fetchColumn();
        assert($etlJobId !== false);

        // Delete existing rows
        echo '    Deleting existing rows';
        $deleteSql = <<<SQL
DELETE FROM $quotedTargetTable
 WHERE _origin_etl_job_id = ?
SQL;
        $statement = $this->database->prepare($deleteSql);
        $statement->execute([$etlJobId]);

        // Insert rows
        echo '    Inserting rows';
        $normalizedQuotedColumnNames = $this->normalizedQuotedColumnNames($columnNames);
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
/*
            $parameters = array_map(function($v){
                return is_null($v)
                    ? null
                    : is_string($v)
                        ? substr($v, 0, 100)
                        : $v;
            }, $parameters);
*/
            $statement->execute($parameters);
            echo '        ' . (array_key_last($rowChunk) + 1) . ' rows' . PHP_EOL;
        }

        // All done
        $this->database->commit();        
    }

    // Private /////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Use SCHEMA and PREFIX to generate table name.
     *
     * @implNote WARNING, this can make names that are too long for the database.
     *
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     *
     * @param string $unqualifiedName
     * @return string
     */
    private function quotedFullyQualifiedTableName(string $unqualifiedName): string
    {
        $qualifiedTableName = ($this->tablePrefix ?? '') . $unqualifiedName;
        if (!empty($this->schema)) {
            $qualifiedTableName = $this->schema . '.' . "`$qualifiedTableName`";
        }
        return $qualifiedTableName;
    }

    /**
     * Turns the columns into unique names of the format which MySQL and SQLite allow as ASCII quoted identifiers
     *
     * @implNote: This will break if a column is named _rowid_
     *
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html identifiers.
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
