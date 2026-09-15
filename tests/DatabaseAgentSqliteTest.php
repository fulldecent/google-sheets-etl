<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class DatabaseAgentSqliteTest extends \PHPUnit\Framework\TestCase
{
    private \PDO $database;

    private DatabaseAgentSqlite $databaseAgent;

    #[\Override]
    protected function setUp(): void
    {
        $this->database = new \PDO('sqlite::memory:', null, null, [
            \PDO::ATTR_PERSISTENT => false,
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_SILENT
        ]);
        $this->databaseAgent = DatabaseAgent::agentForPdo($this->database);
        $this->databaseAgent->setUpAccounting();
    }

    public function testMetadataTablesCreated(): void
    {
        $this->databaseAgent->setUpAccounting();
        $tables = $this->database->query(<<<SQL
            SELECT name FROM sqlite_master
             WHERE type = 'table' AND name LIKE '__meta_%'
             ORDER BY name
            SQL)->fetchAll(\PDO::FETCH_COLUMN);

        self::assertSame(['__meta_etl_jobs', '__meta_spreadsheets'], $tables);
        self::assertSame(1, (int) $this->database->query('PRAGMA foreign_keys')->fetchColumn());
        self::assertSame(\PDO::ERRMODE_EXCEPTION, $this->database->getAttribute(\PDO::ATTR_ERRMODE));
    }

    public function testSpreadsheetAccountingAndOrdering(): void
    {
        self::assertNull($this->databaseAgent->getGreatestModified());
        self::assertNull($this->databaseAgent->getOldestSeen());

        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-01-01T00:00:00Z', 'Sheet A');
        $this->databaseAgent->setSpreadsheetSeen('sheet-b', '2026-02-01T00:00:00Z', 'Sheet B');
        self::assertSame(['2026-02-01T00:00:00Z', 'sheet-b'], $this->databaseAgent->getGreatestModified());
        self::assertSame('sheet-a', $this->databaseAgent->getOldestSeen());

        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-03-01T00:00:00Z', 'Renamed A');
        self::assertSame(['2026-03-01T00:00:00Z', 'sheet-a'], $this->databaseAgent->getGreatestModified());
        self::assertSame(2, (int) $this->database->query('SELECT COUNT(*) FROM __meta_spreadsheets')->fetchColumn());
    }

    public function testCreatesAndLoadsTargetTable(): void
    {
        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-01-01T00:00:00Z', 'Sheet A');
        $this->databaseAgent->createTable('target', ['First Name', 'First Name', '123']);
        $this->databaseAgent->loadSheet(
            'sheet-a',
            'People',
            'target',
            ['First Name', 'First Name', '123'],
            [['Ada', 'Lovelace', 'one'], ['Grace', 'Hopper', 'two']],
            'hash-1',
        );

        $rows = $this->database->query(<<<SQL
            SELECT "first name", col_2, _123 FROM target ORDER BY _origin_row
            SQL)->fetchAll(\PDO::FETCH_NUM);
        self::assertSame([['Ada', 'Lovelace', 'one'], ['Grace', 'Hopper', 'two']], $rows);
    }

    public function testFiltersLoadedJobsAndReplacesChangedRows(): void
    {
        $job = $this->job('sheet-a', 'People', 'target');
        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-01-01T00:00:00Z', 'Sheet A');
        $this->databaseAgent->createTable('target', ['Name']);
        self::assertSame([$job], $this->databaseAgent->filterExtractable([$job]));

        $this->databaseAgent->loadSheet('sheet-a', 'People', 'target', ['Name'], [['Ada']], 'hash-1');
        self::assertSame([], $this->databaseAgent->filterExtractable([$job]));
        self::assertSame('target', $this->database->query('SELECT target_table FROM __meta_etl_jobs')->fetchColumn());

        $this->databaseAgent->loadSheet('sheet-a', 'People', 'target', ['Name'], [['Ignored']], 'hash-1');
        self::assertSame(['Ada'], $this->database->query('SELECT name FROM target')->fetchAll(\PDO::FETCH_COLUMN));

        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-02-01T00:00:00Z', 'Sheet A');
        self::assertSame([$job], $this->databaseAgent->filterExtractable([$job]));
        $this->databaseAgent->loadSheet('sheet-a', 'People', 'target', ['Name'], [['Grace']], 'hash-2');
        self::assertSame(['Grace'], $this->database->query('SELECT name FROM target')->fetchAll(\PDO::FETCH_COLUMN));
    }

    public function testFiltersJobsAcrossBatchBoundary(): void
    {
        $jobs = [];
        for ($index = 0; $index < 401; $index++) {
            $jobs[] = $this->job('sheet-' . $index, 'People', 'target-' . $index);
        }
        $this->databaseAgent->setSpreadsheetSeen('sheet-400', '2026-01-01T00:00:00Z', 'Sheet 400');
        $this->databaseAgent->createTable('target-400', ['Name']);
        $this->databaseAgent->loadSheet('sheet-400', 'People', 'target-400', ['Name'], [['Ada']], 'hash-400');

        $extractable = $this->databaseAgent->filterExtractable($jobs);

        self::assertCount(400, $extractable);
        self::assertFalse(in_array($jobs[400], $extractable, true));
    }

    public function testSameHashReloadsWhenTargetTableChanges(): void
    {
        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-01-01T00:00:00Z', 'Sheet A');
        $this->databaseAgent->createTable('old_target', ['Name']);
        $this->databaseAgent->loadSheet('sheet-a', 'People', 'old_target', ['Name'], [['Ada']], 'same-hash');

        $this->databaseAgent->createTable('new_target', ['Name']);
        $this->databaseAgent->loadSheet('sheet-a', 'People', 'new_target', ['Name'], [['Ada']], 'same-hash');

        self::assertSame(['Ada'], $this->database->query('SELECT name FROM new_target')->fetchAll(\PDO::FETCH_COLUMN));
        self::assertSame(
            'new_target',
            $this->database->query('SELECT target_table FROM __meta_etl_jobs')->fetchColumn(),
        );
    }

    public function testMetadataColumnNamesAreRemapped(): void
    {
        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-01-01T00:00:00Z', 'Sheet A');
        $columns = ['_rowid', '_origin_etl_job_id', '_origin_row'];
        $this->databaseAgent->createTable('target', $columns);
        $this->databaseAgent->loadSheet('sheet-a', 'People', 'target', $columns, [['a', 'b', 'c']], 'hash-1');

        self::assertSame(
            [['a', 'b', 'c']],
            $this->database->query('SELECT col_1, col_2, col_3 FROM target')->fetchAll(\PDO::FETCH_NUM),
        );
    }

    public function testAttachedSchemaAndTablePrefix(): void
    {
        $this->database->exec("ATTACH DATABASE ':memory:' AS warehouse");
        $databaseAgent = DatabaseAgent::agentForPdo($this->database);
        $databaseAgent->schema = 'warehouse';
        $databaseAgent->tablePrefix = 'etl"_';
        $databaseAgent->setUpAccounting();
        $databaseAgent->setSpreadsheetSeen('sheet-a', '2026-01-01T00:00:00Z', 'Sheet A');
        $databaseAgent->createTable('target', ['Name']);
        $databaseAgent->loadSheet('sheet-a', 'People', 'target', ['Name'], [['Ada']], 'hash-1');

        self::assertSame(
            ['Ada'],
            $this->database->query('SELECT name FROM warehouse."etl""_target"')->fetchAll(\PDO::FETCH_COLUMN),
        );
    }

    public function testFailedLoadRollsBack(): void
    {
        $this->databaseAgent->setSpreadsheetSeen('sheet-a', '2026-01-01T00:00:00Z', 'Sheet A');
        $this->databaseAgent->createTable('target', ['Name']);
        $this->databaseAgent->loadSheet('sheet-a', 'People', 'target', ['Name'], [['Ada']], 'hash-1');

        try {
            $this->databaseAgent->loadSheet('sheet-a', 'People', 'target', ['Name'], [['too', 'many']], 'hash-2');
            self::fail('Expected malformed row to fail');
        } catch (\PDOException) {
            self::assertFalse($this->database->inTransaction());
        }

        self::assertSame(['Ada'], $this->database->query('SELECT name FROM target')->fetchAll(\PDO::FETCH_COLUMN));
        self::assertSame(
            'hash-1',
            $this->database->query('SELECT raw_columns_rows_hash FROM __meta_etl_jobs')->fetchColumn(),
        );
    }

    public function testUnknownSpreadsheetLoadRollsBack(): void
    {
        $this->databaseAgent->createTable('target', ['Name']);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Spreadsheet not found: missing');
        try {
            $this->databaseAgent->loadSheet('missing', 'People', 'target', ['Name'], [['Ada']], 'hash-1');
        } finally {
            self::assertFalse($this->database->inTransaction());
            self::assertSame(0, (int) $this->database->query('SELECT COUNT(*) FROM target')->fetchColumn());
        }
    }

    private function job(string $spreadsheetId, string $sheetName, string $targetTable): EtlConfig
    {
        $job = new EtlConfig();
        $job->googleSpreadsheetId = $spreadsheetId;
        $job->sheetName = $sheetName;
        $job->targetTable = $targetTable;
        return $job;
    }
}
