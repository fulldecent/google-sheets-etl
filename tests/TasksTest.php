<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class TasksTest extends \PHPUnit\Framework\TestCase
{
    private GoogleSheetsAgent $googleSheetsAgent;
    private DatabaseAgent $databaseAgent;
    private Tasks $tasks;

    #[\Override]
    protected function setUp(): void
    {
        $this->googleSheetsAgent = $this->createStub(GoogleSheetsAgent::class);
        $this->databaseAgent = $this->createStub(DatabaseAgent::class);
        $this->tasks = new Tasks(
            '',
            $this->createStub(\PDO::class),
            $this->googleSheetsAgent,
            $this->databaseAgent,
        );
    }

    public function testNoPreviouslySeenSpreadsheetIsAccessible(): void
    {
        $this->databaseAgent->method('getOldestSeen')->willReturn(null);

        self::assertTrue($this->tasks->verifyOldestSpreadsheet());
    }

    public function testMissingSpreadsheetIsNotAccessible(): void
    {
        $this->databaseAgent->method('getOldestSeen')->willReturn('missing-sheet');
        $this->googleSheetsAgent->method('getSpreadsheet')->willThrowException(
            new \Google\Service\Exception('Not found', 404),
        );

        self::assertFalse($this->tasks->verifyOldestSpreadsheet());
    }

    public function testNonNotFoundGoogleErrorIsRethrown(): void
    {
        $exception = new \Google\Service\Exception('Service unavailable', 503);
        $this->databaseAgent->method('getOldestSeen')->willReturn('sheet-a');
        $this->googleSheetsAgent->method('getSpreadsheet')->willThrowException($exception);

        $this->expectExceptionObject($exception);
        $this->tasks->verifyOldestSpreadsheet();
    }

    public function testAccessibleSpreadsheetIsRecorded(): void
    {
        $spreadsheet = (object) ['modifiedTime' => '2026-09-15T00:00:00Z', 'name' => 'Planning'];
        $databaseAgent = $this->createMock(DatabaseAgent::class);
        $googleSheetsAgent = $this->createMock(GoogleSheetsAgent::class);
        $databaseAgent->method('getOldestSeen')->willReturn('sheet-a');
        $googleSheetsAgent->expects(self::once())
            ->method('getSpreadsheet')
            ->with('sheet-a')
            ->willReturn($spreadsheet);
        $databaseAgent->expects(self::once())
            ->method('setSpreadsheetSeen')
            ->with('sheet-a', $spreadsheet->modifiedTime, $spreadsheet->name);
        $tasks = new Tasks('', $this->createStub(\PDO::class), $googleSheetsAgent, $databaseAgent);

        self::assertTrue($tasks->verifyOldestSpreadsheet());
    }
}
