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

    abstract function storeAccounting(string $spreadsheetId, string $sheetName, string $modifiedTime, string $unqualifiedTableName);
    
    /**
     * The latest modified time in the database
     *
     * @see https://tools.ietf.org/html/rfc3339
     * 
     * @return ?string The time, or a default value before Google Drive existed
     */
    function getLatestMotidifedTime(): ?string {
echo 'TODO: refactor this so that it get the last spreadsheet that was fully loaded. As current the way this is used it can skip other sheets on reload if one sheet causes a crash' . PHP_EOL;
        $quotedFullyQualifiedMetaTableName = $this->getQuotedFullyQualifiedMetaTableName();
        $sql = <<<SQL
SELECT MAX(last_modified)
  FROM $quotedFullyQualifiedMetaTableName
 WHERE last_modified = last_loaded
SQL;
        return $this->database->query($sql)->fetchColumn();
    }

    /**
     * @param string $date YYYY-MM-DD format
     */
    function getGreatestIdWithAuthorizationCheckedSince(string $date)
    {
        $quotedFullyQualifiedMetaTableName = $this->getQuotedFullyQualifiedMetaTableName();
        $sql = <<<SQL
SELECT MAX(spreadsheet_id)
  FROM $quotedFullyQualifiedMetaTableName
 WHERE last_authorization_checked >= ?
SQL;
        $statement = $this->database->prepare($sql);
        $statement->execute([$date]);
        return $statement->fetchColumn();
    }

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
        $this->storeAccounting($spreadsheetId, $sheetName, $modifiedTime, $unqualifiedTableName);
        $this->database->commit();
    }
}