<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for spreadsheets in a PDO database
 *
 * A spreadsheet contains multiple sheets, each is a row/column data structure.
 *
 * You can imagine accounting is approximately:
 *
 * TABLE spreadsheets
 *   * __rowid (int)
 *     * Key
 *     * Databases require an increasing value to make INSERTs fast
 *   * spreadsheet_id (string |^[0-9a-z_/-]{44}$|i)
 *     * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets
 *     * The allowable set of spreadsheetids is currently undefined behavior per Google API documentation (issue reported)
 *     * This regex is an estimate
 *   * last_modified (string, RFC 3339 date-time)
 *     * https://developers.google.com/drive/api/v3/reference/files
 *   * last_authorization_checked (string YYYY-MM-DD HH:MM:SS)
 *     * Unix timestamp, system time
 *   * last_loaded (string, RFC 3339 date-time, nullable)
 *     * Matches last_modified when fully loaded
 *   * CONSTRAINT spreadsheet_id UNIQUE
 *
 * TABLE sheets
 *   * __rowid (int)
 *     * Key
 *     * Databases require an increasing value to make INSERTs fast
 *   * spreadsheet_rowid
 *     * Match above table's key
 *   * sheet_name (string)
 *     * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#SheetProperties
 *     * The allowable set of sheet names is currently undefined behavior per Google API documentation (issue reported)
 *   * last_loaded (string, RFC 3339 date-time, nullable)
 *     * Matches last_modified when loaded
 *   * table_name (string)
 *     * Pointer to the table where this sheet is stored in the data store
 *   * CONSTRAINT spreadsheet_id, sheet_name UNIQUE
 */
abstract class DatabaseAgent
{
    /**
     * Schema prefix, like 'otherdatabase.'
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

    public static function agentForPdo(\PDO $newDatabase): DatabaseAgent
    {
        switch ($newDatabase->getAttribute(\PDO::ATTR_DRIVER_NAME)) {
            case 'sqlite':
                return new DatabaseAgentSqlite($newDatabase);
                break;
            case 'mysql':
                return new DatabaseAgentMysql($newDatabase);
                break;
            default:
                echo "Unexpected driver: " . $newDatabase->getAttribute(\PDO::ATTR_DRIVER_NAME);
                exit(1);
        }
    }
    
    protected function __construct(\PDO $newDatabase)
    {
        $this->database = $newDatabase;
        $this->loadTime = date('Y-m-d H:i:s');
        $this->setUpAccounting();
    }

    // Getters /////////////////////////////////////////////////////////////////

    /**
     * @return string The time this script loaded, in YYYY-MM-DD HH:MM-SS format
     */
    public function getLoadTime(): string
    {
        return $this->loadTime;
    }

    /**
     * Get table name for a sheet
     *
     * @param $spreadsheetId string the spreadsheet ID to query
     * @param $sheetName string the sheet name to query
     * @return the table name for the sheet, or null if sheet is not loaded
     */
    abstract public function getTableNameForSheet(string $spreadsheetId, string $sheetName): ?string;
    
    /**
     * For all spreadsheets which are fully loaded, get the one with the
     * greatest lexical order of (modified_time, spreadsheet_id), or null if no
     * spreadsheets are loaded
     *
     * @see https://tools.ietf.org/html/rfc3339
     *
     * @return array like ['2015-01-01 03:04:05', '349u948k945kd43-k35529_298k938']
     */
    abstract public function getGreatestModifiedAndIdLoaded(): ?array;

    /**
     * For all spreadsheets with authorization confirmed on or after the given
     * date, get the greatest spreadsheet ID
     *
     * @param string int a Unix timestamp
     * @return ?string spreadsheet ID or null
     */
    abstract public function getGreatestIdWithAuthorizationCheckedSince(int $since): ?string;

    /**
     * Get all spreadsheets which did not have authorization confirmed on or
     * after the given time
     *
     * @param string int a Unix timestamp
     * @param int limit a maximum quantity of results to return
     * @return array spreadsheet IDs in order starting with the least and
     *               including up to LIMIT number of rows
     */
    abstract public function getIdsWithAuthorizationNotCheckedSince(string $since, int $limit): array;

    // Accounting //////////////////////////////////////////////////////////////

    /**
     * The accounting must be set up before any other methods are called
     *
     * @apiSpec Calling this method twice shall not cause any data loss or any
     *          error.
     */
    abstract protected function setUpAccounting();

    /**
     * Account that a spreadsheet is authorized
     */
    abstract public function accountSpreadsheetAuthorized(string $spreadsheetId, string $lastModified);
    
    /**
     * Account that a spreadsheet is fully loaded (after all sheets loaded)
     */
    abstract public function accountSpreadsheetLoaded(string $spreadsheetId, string $lastModified);

    // Data store //////////////////////////////////////////////////////////////

    /**
     * Removes sheet and accounting, if exists, and load and account for sheet
     *
     * @apiSpec This operation shall be atomic, no partial effect may occur on
     *          the database if program is prematurely exited.
     */
    abstract public function loadAndAccountSheet(string $spreadsheetId, string $sheetName, string $tableName, string $modifiedTime, array $columns, array $rows);

    /**
     * Removes sheets and accounting for any sheets that do not have the latest
     * known data loaded
     */
    abstract public function removeOutdatedSheets(string $spreadsheetId);

    /**
     * Removes sheets and accounting for given spreadsheet
     */
    abstract public function removeSpreadsheet(string $spreadsheetId);
}
