<?php
namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for putting CSV files into a PDO database
 */
abstract class DatabaseAgent
{
    protected /* \PDO */ $database;
    protected /* ?string */ $loadTime;
    
    function __construct(\PDO $newDatabase)
    {
        $this->database = $newDatabase;
        $this->loadTime = date('Y-m-d H:i:s');
    }

    abstract function setupDatabase();

    abstract function createTable(string $unqualifiedTableName, array $columns);

    abstract function insertRows(string $quotedFullyQualifiedTableName, array $rows);

    /**
     * Load one Google Sheets grid sheet into the database
     * 
     * @param string $spreadsheetId
     * @param string $sheetName
     * @param string $modifiedTime
     * @param array $columns
     * @param array $rows
     * @param string $unqualifiedTableName
     */
    function loadSheet(string $spreadsheetId, string $sheetName, string $modifiedTime, array $columns, array $rows, string $unqualifiedTableName)
    {
        $this->database->beginTransaction();
        $this->createTable($unqualifiedTableName, $columns);
        $this->insertRows($unqualifiedTableName, $rows);
        $this->storeALocation($spreadsheetId, $sheetName, $modifiedTime, $unqualifiedTableName);
        $this->database->commit();
    }

    abstract function storeALocation(string $spreadsheetId, string $sheetName, string $modifiedTime, string $unqualifiedTableName);

    /**
     * If the Google Sheet is in the database then update the authorization
     * confirmed time to now.
     */
    abstract function storeAuthorizationConfirmation(string $spreadsheetId);

    /**
     * The latest modified time in the database
     *
     * @see https://tools.ietf.org/html/rfc3339
     * 
     * @return ?string the time in RFC3339 format, or null if there are none
     */
    abstract function getLatestMotidifedTime(): ?string;

    /**
     * For all Google Sheets with authorization confirmed on or after the given
     * date, return the greatest ID
     * 
     * @param string $date YYYY-MM-DD format
     */
    abstract function getGreatestIdWithAuthorizationCheckedSince(string $date): ?string;
}