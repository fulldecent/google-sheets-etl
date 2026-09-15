<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class DatabaseAgentTest extends \PHPUnit\Framework\TestCase
{
    public function testRoutesSqliteConnection(): void
    {
        $database = new \PDO('sqlite::memory:');

        self::assertInstanceOf(DatabaseAgentSqlite::class, DatabaseAgent::agentForPdo($database));
    }

    public function testRejectsUnsupportedDriver(): void
    {
        $database = $this->createMock(\PDO::class);
        $database->expects(self::once())
            ->method('getAttribute')
            ->with(\PDO::ATTR_DRIVER_NAME)
            ->willReturn('pgsql');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported PDO driver: pgsql');
        DatabaseAgent::agentForPdo($database);
    }
}
