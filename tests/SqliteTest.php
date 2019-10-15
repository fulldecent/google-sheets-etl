<?php
namespace fulldecent\GoogleSheetsEtl;

require_once __DIR__ . '/../vendor/autoload.php';

class DatabaseAgentSqliteTest extends \PHPUnit_Framework_TestCase
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
        $this->databaseAgent = DatabaseAgent::AgentForPDO($this->database);
    }

    public function testMetadataTableCreated()
    {
        $result = $this->database->query('SELECT COUNT(*) FROM sqlite_master WHERE name ="__meta_spreadsheets"')->fetchColumn();
        $this->assertEquals(1, $result);
    }

    public function testGetTableNameForSheetNonExistant()
    {
        $spreadsheetId = 'aaaa';
        $result = $this->databaseAgent->getTableNameForSheet($spreadsheetId, 'nonexistantsheet');
        $this->assertEquals(null, $result);
    }

    public function testAddedData()
    {
        $headers = ['col 1', 'col 2'];
        $data = [ 
            ['a', 'b'],
            ['c', 'd']
        ];
        $spreadsheetId = '17azPU9lRfRMzFFbwj4SREN-jbzCalUvUWpDHAYCZ1vs';
        $sheetName = 'sname';
    }
}
