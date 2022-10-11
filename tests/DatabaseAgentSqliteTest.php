<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class DatabaseAgentSqliteTest extends \PHPUnit\Framework\TestCase
{
    /** @var \PDO */
    private $database;

    /** @var DatabaseAgentSqlite */
    private $databaseAgent;

    protected function setUp(): void
    {
        $this->database = new \PDO('sqlite::memory:', null, null, [
            \PDO::ATTR_PERSISTENT => false,
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ]);
        $this->databaseAgent = DatabaseAgent::agentForPdo($this->database);
        $this->databaseAgent->setUpAccounting();
    }

    public function testMetadataTableCreated()
    {
        $sql = 'SELECT COUNT(*) FROM sqlite_master WHERE name ="__meta_spreadsheets"';
        $result = $this->database->query($sql)->fetchColumn();
        $this->assertEquals(1, $result);
    }
}
