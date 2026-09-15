<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class GoogleSheetsAgentTest extends \PHPUnit\Framework\TestCase
{
    public function testRejectsInvalidCredentialsJson(): void
    {
        $credentialsFile = tempnam(sys_get_temp_dir(), 'google-sheets-etl-');
        self::assertNotFalse($credentialsFile);
        file_put_contents($credentialsFile, '{invalid');

        try {
            $this->expectException(\JsonException::class);
            $this->expectExceptionMessage('Invalid credentials JSON: Syntax error');
            new GoogleSheetsAgent($credentialsFile);
        } finally {
            unlink($credentialsFile);
        }
    }
}
