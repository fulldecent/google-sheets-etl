<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class EtlConfigTest extends \PHPUnit\Framework\TestCase
{
    private string $configFile = '';

    #[\Override]
    protected function tearDown(): void
    {
        if ($this->configFile !== '' && is_file($this->configFile)) {
            unlink($this->configFile);
        }
    }

    public function testZeroColumnIndexIsAccepted(): void
    {
        $configs = EtlConfig::fromFile($this->writeConfig(['name' => 0, 'email' => 'Email']));

        self::assertCount(1, $configs);
        self::assertSame(['name' => 0, 'email' => 'Email'], $configs[0]->columnMapping);
    }

    public function testNegativeColumnIndexIsRejected(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('columnMapping integer values must be zero or greater');

        EtlConfig::fromFile($this->writeConfig(['name' => -1]));
    }

    /**
     * @param array<string, int|string> $columnMapping
     */
    private function writeConfig(array $columnMapping): string
    {
        $file = tempnam(sys_get_temp_dir(), 'etl');
        if ($file === false) {
            self::fail('Unable to create a temporary configuration file');
        }
        $this->configFile = $file;
        $written = file_put_contents($file, json_encode([
            '$schema' => './config-schema.json',
            str_repeat('a', 44) => [
                'People' => [
                    'targetTable' => 'people',
                    'columnMapping' => $columnMapping,
                ],
            ],
        ], JSON_THROW_ON_ERROR));
        if ($written === false) {
            self::fail('Unable to write a temporary configuration file');
        }

        return $file;
    }
}
