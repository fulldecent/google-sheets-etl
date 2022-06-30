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
 * TABLE spreadsheets
 *   * id (int) PRIMARY KEY
 *     * Key
 *     * Databases require an increasing value to make INSERTs fast
 *   * google_spreadsheet_id (string |^[a-zA-Z0-9-_]{44}$|i) (UNIQUE)
 *     * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets
 *     * https://developers.google.com/sheets/api/guides/concepts
 *     * The allowable set of spreadsheetids is currently undefined behavior per Google API documentation (issue reported)
 *     * This regex is an estimate
 *   * google_spreadsheet_name (string)
 *     * https://stackoverflow.com/questions/62050607/what-is-a-string-in-the-google-drive-api
 *   * google_modified (string, RFC 3339 date-time)
 *     * https://developers.google.com/drive/api/v3/reference/files
 *   * last_seen (int)
 *     * Unix timestamp, system time
 *     * This is the last time we confirmed access to this file
 *
 * TABLE etl_jobs
 *   * id (int) PRIMARY KEY
 *     * Key
 *     * Databases require an increasing value to make INSERTs fast
 *   * spreadsheet_id FOREIGN KEY spreadsheets.id
 *   * sheet_name (string)
 *     * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#SheetProperties
 *     * The allowable set of sheet names is currently undefined behavior per Google API documentation (issue reported)
 *   * target_table (string)
 *     * Pointer to the table where this sheet is stored in the data store
 *   * google_modified
 *     * Matches spreadsheets.google_modified when loaded
 *   * CONSTRAINT sheet_name (spreadsheet_id, sheet_name)
 *
 * TABLE target_table (this will have various names)
 *   * _rowid (int) PRIMARY KEY
 *   * _origin_etl_job_id FOREIGN KEY etl_jobs.id
 *   * _origin_row (int)
 *   * CONSTRAINT _origin_row (_origin_etl_job_id, _origin_row)
 */
abstract class DatabaseAgent
{
    /**
     * Schema prefix, like 'otherdatabase'
     * @var ?string
     */
    public $schema;

    /**
     * Prefix for every table name, beware of maximum table name length
     * @var ?string
     */
    public $tablePrefix;

    protected /* \PDO */ $database;
    protected /* ?string */ $loadTime;

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
     * @return int The time this script loaded
     */
    final public function getLoadTime(): int
    {
        return $this->loadTime;
    }

    /**
     * For all spreadsheets which are partially or fully loaded, get the one
     * with the greatest lexical order of (google_modified,
     * google_spreadsheet_id), or null if no spreadsheets are loaded
     *
     * @see https://tools.ietf.org/html/rfc3339
     *
     * @return array like ['2015-01-01 03:04:05', '1-Dcs8ZYoyz82xkjkv3tIbSCAJOOpouXur4dwql4TqiY']
     */
    abstract public function getGreatestModifiedAndIdLoaded(): ?array;

    /**
     * For all spreadsheets seen on or after the given date, get the greatest
     * Google spreadsheet ID
     *
     * @param integer $since a Unix timestamp
     * @return string|null
     */
    abstract public function getGreatestIdSeenSince(int $since): ?string;

    /**
     * Get all spreadsheets which were not seen after the given time
     *
     * @param integer $since  a Unix timestamp
     * @param integer $limit  limit a maximum quantity of results to return
     * @return array          Google spreadsheet IDs in order starting with the
     *                        least and including up to LIMIT number of rows
     */
    abstract public function getIdsNotSeenSince(int $since, int $limit): array;

    /**
     * Get ETL details for a specific spreadsheet and sheet
     *
     * @param string $googleSpreadsheetId  specified Google spreadsheet ID
     * @param string $sheetName            specified sheet name
     * @return \stdClass|null              ETL information
     */
    abstract public function getEtl(string $googleSpreadsheetId, string $sheetName): ?\stdClass;

    // Accounting //////////////////////////////////////////////////////////////

    /**
     * The accounting must be set up before any other methods are called
     *
     * @apiSpec Calling this method twice shall not cause data loss or error.
     */
    abstract protected function setUpAccounting();

    /**
     * Account that a spreadsheet is seen, this confirms we have access
     */
    abstract public function accountSpreadsheetSeen(string $googleSpreadsheetId, string $googleModified, string $name);

    // Data store //////////////////////////////////////////////////////////////

    /**
     * Removes sheet and accounting, if it exists, and loads and accounts for sheet
     *
     * @apiSpec This operation shall be atomic, no partial effect may occur on
     *          the database if program is prematurely exited.
     */
    abstract public function loadAndAccountSheet(string $googleSpreadsheetId, string $sheetName, string $targetTable, string $googleModified, array $columnNames, array $rows);
}
