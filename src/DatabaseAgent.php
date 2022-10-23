<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

use stdClass;

/**
 * A data store and accounting for spreadsheets in a PDO database
 *
 * A spreadsheet contains multiple sheets, each is a row/column data structure.
 *
 * You can imagine accounting is approximately:
 *
 * - spreadsheets (spreadsheets confirmed to be accessible)
 *   - id (int) PRIMARY KEY
 *     - Databases require an increasing value to make INSERTs fast
 *   - google_spreadsheet_id (string) UNIQUE
 *     - Format is probably |^[a-zA-Z0-9-_]{44}$|i, but that's not documented
 *     - https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets
 *     - https://developers.google.com/sheets/api/guides/concepts
 *   - google_spreadsheet_name (string)
 *     - https://stackoverflow.com/questions/62050607/what-is-a-string-in-the-google-drive-api
 *   - google_modified (string, RFC 3339 date-time)
 *     - https://developers.google.com/drive/api/v3/reference/files
 *   - last_seen (int)
 *     - Unix timestamp, system time
 *     - This is the last time we confirmed access to this file
 *
 * - etl_jobs
 *   - id (int) PRIMARY KEY
 *     - Databases require an increasing value to make INSERTs fast
 *   - spreadsheet_id FOREIGN KEY spreadsheets.id
 *   - sheet_name (string)
 *     - Google does not define the maximum length of a sheet name
 *     - https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#SheetProperties
 *   - target_table (string)
 *     - Pointer to the table where this sheet is stored in the data store
 *   - google_modified
 *     - Matches spreadsheets.google_modified when loaded
 *   - raw_columns_rows_hash (string)
 *     - SHA256 hash of JSON string of the columns and rows loaded
 *   - CONSTRAINT UNIQUE sheet_name (spreadsheet_id, sheet_name)
 *
 * - target_table (this will have various names)
 *   - _origin_etl_job_id FOREIGN KEY etl_jobs.id
 *   - _origin_row (int)
 *   - CONSTRAINT UNIQUE _origin_row (_origin_etl_job_id, _origin_row)
 */
abstract class DatabaseAgent
{
    /**
     * Schema prefix, like 'otherdatabase'
     */
    public ?string $schema;

    /**
     * Prefix for every table name, beware of maximum table name length
     */
    public ?string $tablePrefix;

    protected \PDO $database;

    /** 
     * The time this script was started
     */
    protected int $loadTime;

    final public static function agentForPdo(\PDO $newDatabase): DatabaseAgent
    {
        switch ($newDatabase->getAttribute(\PDO::ATTR_DRIVER_NAME)) {
            case 'sqlite':
                return new DatabaseAgentSqlite($newDatabase);
            case 'mysql':
                return new DatabaseAgentMysql($newDatabase);
            default:
                echo "Unexpected driver: " . $newDatabase->getAttribute(\PDO::ATTR_DRIVER_NAME);
                exit(1);
        }
    }

    protected function __construct(\PDO $newDatabase)
    {
        $this->database = $newDatabase;
        $this->loadTime = time();
    }

    // Getters /////////////////////////////////////////////////////////////////

    /**
     * For all spreadsheets which were confirmed accessible, get the one with the greatest lexical order of
     * (google_modified, google_spreadsheet_id), or null if no spreadsheets were seen
     *
     * @see https://tools.ietf.org/html/rfc3339
     *
     * @return ?array like ['2015-01-01 03:04:05', '1-Dcs8ZYoyz82xkjkv3tIbSCAJOOpouXur4dwql4TqiY']
     */
    abstract public function getGreatestModified(): ?array;

    /**
     * For all spreadsheets which were confirmed accessible, get the one that was confirmed the longest ago, or null if
     * no spreadsheets were seen
     *
     * @return ?string The google_spreadsheet_id of the oldest-seen spreadsheet
     */
    abstract public function getOldestSeen(): ?string;

    /**
     * Filter ETL list to ones that can extract updated spreadsheets
     * 
     * @param array<EtlConfig> $jobs to filter
     * @return array<EtlConfig> $jobs which may have new data (the spreadsheet was updated, not necessarily that sheet)
     */
    abstract public function filterExtractable(array $jobs): array;

    // Setters /////////////////////////////////////////////////////////////////

    /**
     * Accounting must be set up before any other methods are called
     *
     * @apiSpec Calling this method twice shall not cause data loss or error.
     */
    abstract public function setUpAccounting(): void;

    /**
     * Account that a spreadsheet is seen, this confirms we have access
     */
    abstract public function setSpreadsheetSeen(string $googleSpreadsheetId, string $googleModified, string $name): void;

    /**
     * Account that ETL was performed as of spreadsheets.google_modified time; and if rows are different than before
     * then actually load the data
     *
     * @apiSpec This operation shall be atomic, no partial effect may occur on the database if program is prematurely
     *          exited.
     */
    abstract public function loadSheet(
        string $googleSpreadsheetId,
        string $sheetName,
        string $targetTable,
        array $columnNames,
        array $rows,
        string $hash,
    ): void;
}
