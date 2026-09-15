<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for spreadsheets in a SQLite database
 */
class DatabaseAgentSqlite extends DatabaseAgent
{
    public const string SPREADSHEETS_TABLE = '__meta_spreadsheets';
    public const string ETL_JOBS_TABLE = '__meta_etl_jobs';
    private const array RESERVED_COLUMN_NAMES = ['_rowid', '_origin_etl_job_id', '_origin_row'];

    protected function __construct(\PDO $newDatabase)
    {
        parent::__construct($newDatabase);
        $this->database->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    #[\Override]
    public function getGreatestModified(): ?array
    {
        $spreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $row = $this->database->query(<<<SQL
SELECT google_modified, google_spreadsheet_id
  FROM $spreadsheetsTable
 ORDER BY google_modified DESC, google_spreadsheet_id DESC
 LIMIT 1
SQL)->fetch(\PDO::FETCH_NUM);
        return $row === false ? null : $row;
    }

    #[\Override]
    public function getOldestSeen(): ?string
    {
        $spreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $spreadsheetId = $this->database->query(<<<SQL
SELECT google_spreadsheet_id
  FROM $spreadsheetsTable
 ORDER BY last_seen, id
 LIMIT 1
SQL)->fetchColumn();
        return $spreadsheetId === false ? null : $spreadsheetId;
    }

    #[\Override]
    public function filterExtractable(array $jobs): array
    {
        if ($jobs === []) {
            return [];
        }

        $spreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $etlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $loadedJobs = [];
        foreach (array_chunk($jobs, 400) as $jobChunk) {
            $placeholders = implode(', ', array_fill(0, count($jobChunk), '(?, ?)'));
            $statement = $this->database->prepare(<<<SQL
SELECT spreadsheets.google_spreadsheet_id, etl_jobs.sheet_name
  FROM $spreadsheetsTable spreadsheets
  JOIN $etlJobsTable etl_jobs
    ON etl_jobs.spreadsheet_id = spreadsheets.id
 WHERE (spreadsheets.google_spreadsheet_id, etl_jobs.sheet_name) IN (VALUES $placeholders)
   AND spreadsheets.google_modified = etl_jobs.google_modified
SQL);
            $parameters = [];
            foreach ($jobChunk as $job) {
                array_push($parameters, $job->googleSpreadsheetId, $job->sheetName);
            }
            $statement->execute($parameters);
            foreach ($statement->fetchAll(\PDO::FETCH_NUM) as [$spreadsheetId, $sheetName]) {
                $loadedJobs[$this->jobKey($spreadsheetId, $sheetName)] = true;
            }
        }

        return array_values(array_filter(
            $jobs,
            fn (EtlConfig $job): bool => !isset($loadedJobs[$this->jobKey($job->googleSpreadsheetId, $job->sheetName)])
        ));
    }

    #[\Override]
    public function setUpAccounting(): void
    {
        $this->database->exec('PRAGMA foreign_keys = ON');
        $spreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $etlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $spreadsheetsTableReference = $this->quoteIdentifier($this->tableName(self::SPREADSHEETS_TABLE));

        $this->database->exec(<<<SQL
CREATE TABLE IF NOT EXISTS $spreadsheetsTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    google_spreadsheet_id TEXT NOT NULL UNIQUE,
    google_modified TEXT NOT NULL,
    google_spreadsheet_name TEXT NOT NULL,
    last_seen INTEGER NOT NULL
)
SQL);
        $this->database->exec(<<<SQL
CREATE TABLE IF NOT EXISTS $etlJobsTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    spreadsheet_id INTEGER NOT NULL,
    sheet_name TEXT NOT NULL,
    target_table TEXT NOT NULL,
    google_modified TEXT NOT NULL,
    raw_columns_rows_hash TEXT NOT NULL,
    UNIQUE (spreadsheet_id, sheet_name),
    FOREIGN KEY (spreadsheet_id) REFERENCES $spreadsheetsTableReference(id)
)
SQL);
    }

    #[\Override]
    public function setSpreadsheetSeen(string $googleSpreadsheetId, string $googleModified, string $name): void
    {
        $spreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $this->database->prepare(<<<SQL
INSERT INTO $spreadsheetsTable
    (google_spreadsheet_id, google_modified, google_spreadsheet_name, last_seen)
VALUES
    (:google_spreadsheet_id, :google_modified, :google_spreadsheet_name, :last_seen)
ON CONFLICT (google_spreadsheet_id) DO UPDATE SET
    google_modified = excluded.google_modified,
    google_spreadsheet_name = excluded.google_spreadsheet_name,
    last_seen = excluded.last_seen
SQL)->execute([
                'google_spreadsheet_id' => $googleSpreadsheetId,
                'google_modified' => $googleModified,
                'google_spreadsheet_name' => $name,
                'last_seen' => $this->loadTime,
            ]);
    }

    #[\Override]
    public function createTable(string $targetTable, array $columnNames): void
    {
        $quotedTargetTable = $this->quotedFullyQualifiedTableName($targetTable);
        $etlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $etlJobsTableReference = $this->quoteIdentifier($this->tableName(self::ETL_JOBS_TABLE));
        $this->database->exec(<<<SQL
    CREATE TABLE IF NOT EXISTS $quotedTargetTable (
    _rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    _origin_etl_job_id INTEGER NOT NULL,
    _origin_row INTEGER NOT NULL,
    UNIQUE (_origin_etl_job_id, _origin_row),
    FOREIGN KEY (_origin_etl_job_id) REFERENCES $etlJobsTableReference(id)
)
SQL);

        $existingColumns = $this->database->query($this->tableInfoPragma($targetTable))
            ->fetchAll(\PDO::FETCH_COLUMN, 1);
        foreach ($this->normalizedColumnNames($columnNames) as $columnName) {
            if (!in_array($columnName, $existingColumns, true)) {
                $quotedColumnName = $this->quoteIdentifier($columnName);
                $this->database->exec("ALTER TABLE $quotedTargetTable ADD COLUMN $quotedColumnName TEXT");
            }
        }
    }

    #[\Override]
    public function loadSheet(
        string $googleSpreadsheetId,
        string $sheetName,
        string $targetTable,
        array $columnNames,
        array $rows,
        string $hash,
    ): void {
        $spreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $etlJobsTable = $this->quotedFullyQualifiedTableName(self::ETL_JOBS_TABLE);
        $quotedTargetTable = $this->quotedFullyQualifiedTableName($targetTable);

        $this->database->beginTransaction();
        try {
            $statement = $this->database->prepare(<<<SQL
SELECT etl_jobs.id, etl_jobs.raw_columns_rows_hash, etl_jobs.target_table
    FROM $etlJobsTable etl_jobs
    JOIN $spreadsheetsTable spreadsheets
        ON spreadsheets.id = etl_jobs.spreadsheet_id
 WHERE spreadsheets.google_spreadsheet_id = :google_spreadsheet_id
     AND etl_jobs.sheet_name = :sheet_name
SQL);
            $statement->execute([
                'google_spreadsheet_id' => $googleSpreadsheetId,
                'sheet_name' => $sheetName,
            ]);
            $existingJob = $statement->fetch(\PDO::FETCH_ASSOC);

            $statement = $this->database->prepare(<<<SQL
INSERT INTO $etlJobsTable
    (spreadsheet_id, sheet_name, target_table, google_modified, raw_columns_rows_hash)
SELECT id, :sheet_name, :target_table, google_modified, :raw_columns_rows_hash
  FROM $spreadsheetsTable
 WHERE google_spreadsheet_id = :google_spreadsheet_id
ON CONFLICT (spreadsheet_id, sheet_name) DO UPDATE SET
    target_table = excluded.target_table,
    google_modified = excluded.google_modified,
    raw_columns_rows_hash = excluded.raw_columns_rows_hash
SQL);
            $statement->execute([
                'google_spreadsheet_id' => $googleSpreadsheetId,
                'sheet_name' => $sheetName,
                'target_table' => $targetTable,
                'raw_columns_rows_hash' => $hash,
            ]);
            if ($statement->rowCount() !== 1) {
                throw new \RuntimeException("Spreadsheet not found: $googleSpreadsheetId");
            }

            if (($existingJob['raw_columns_rows_hash'] ?? null) === $hash
                && ($existingJob['target_table'] ?? null) === $targetTable
            ) {
                $this->database->commit();
                return;
            }

            $etlJobId = $existingJob['id'] ?? (int) $this->database->lastInsertId();
            $this->database->prepare("DELETE FROM $quotedTargetTable WHERE _origin_etl_job_id = ?")
                ->execute([$etlJobId]);

            $quotedColumns = array_map([$this, 'quoteIdentifier'], $this->normalizedColumnNames($columnNames));
            $insertColumns = implode(', ', array_merge(['_origin_etl_job_id', '_origin_row'], $quotedColumns));
            $placeholders = implode(', ', array_fill(0, count($columnNames) + 2, '?'));
            $statement = $this->database->prepare(
                "INSERT INTO $quotedTargetTable ($insertColumns) VALUES ($placeholders)"
            );
            foreach ($rows as $rowNumber => $row) {
                $statement->execute([$etlJobId, $rowNumber, ...$row]);
            }
            $this->database->commit();
        } catch (\Throwable $exception) {
            if ($this->database->inTransaction()) {
                $this->database->rollBack();
            }
            throw $exception;
        }
    }

    private function quotedFullyQualifiedTableName(string $tableName): string
    {
        $tableName = $this->quoteIdentifier($this->tableName($tableName));
        if (!empty($this->schema)) {
            return $this->quoteIdentifier($this->schema) . '.' . $tableName;
        }
        return $tableName;
    }

    private function normalizedColumnNames(array $columns): array
    {
        $normalized = [];
        foreach ($columns as $index => $column) {
            $column = strtolower((string) iconv('UTF-8', 'ASCII//TRANSLIT', $column));
            $column = trim((string) preg_replace('/[^a-z0-9_ ]/', '', $column));
            if (!preg_match('/^[a-z_]/', $column)) {
                $column = '_' . $column;
            }
            if (preg_match('/^col_[0-9]+$/', $column)
                || $column === ''
                || in_array($column, self::RESERVED_COLUMN_NAMES, true)
                || in_array($column, $normalized, true)
            ) {
                $column = 'col_' . ($index + 1);
            }
            $normalized[] = $column;
        }
        return $normalized;
    }

    private function tableInfoPragma(string $tableName): string
    {
        $quotedTableName = $this->quoteIdentifier($this->tableName($tableName));
        if (!empty($this->schema)) {
            return 'PRAGMA ' . $this->quoteIdentifier($this->schema) . ".table_info($quotedTableName)";
        }
        return "PRAGMA table_info($quotedTableName)";
    }

    private function tableName(string $tableName): string
    {
        return ($this->tablePrefix ?? '') . $tableName;
    }

    private function jobKey(string $spreadsheetId, string $sheetName): string
    {
        return $spreadsheetId . "\0" . $sheetName;
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
