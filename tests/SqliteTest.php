<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class DatabaseAgentSqliteTest extends \PHPUnit\Framework\TestCase
{
    /** @var \PDO */
    private $database;

    /** @var DatabaseAgentSqlite */
    private $databaseAgent;

    protected function setUp()
    {
        $this->database = new \PDO('sqlite::memory:', null, null, [
            \PDO::ATTR_PERSISTENT => false,
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ]);
        $this->databaseAgent = DatabaseAgent::agentForPdo($this->database);
    }

    public function testMetadataTableCreated()
    {
        $sql = 'SELECT COUNT(*) FROM sqlite_master WHERE name ="__meta_spreadsheets"';
        $result = $this->database->query($sql)->fetchColumn();
        $this->assertEquals(1, $result);
    }

    public function testGetTableNameForSheetNonExistant()
    {
        $spreadsheetId = 'aaaa';
        $result = $this->databaseAgent->getTableNameForSheet($spreadsheetId, 'nonexistantsheet');
        $this->assertEquals(null, $result);
    }
}
