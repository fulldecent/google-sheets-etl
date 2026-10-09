<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * A data structure for configuring ETL loads
 */
class EtlConfig
{
    public string $googleSpreadsheetId;
    public string $sheetName;
    public string $targetTable;

    /** @var array<string, int|string> */
    public array $columnMapping = [];

    public int $headerRow = 0;
    public int $skipRows = 1;

    /**
     * Example:
     * {
     *     "$schema": "./config-schema.json",
     *     "1b33RL2nQJxdaHYxVmkk4lo3K1IKjSD3_ggnokrZCkx8": {
     *     "2019 Expirations": {
     *     "targetTable": "certification-course-renewals-2019",
     *     "columnMapping": {"out1": "in1", "out2": 2},
     *     "headerRow": 0,
     *     "skipRows": 1
     * }
     *
     * @param string $file JSON configuration file conforming to config-schema.json
     * @return list<EtlConfig>
     */
    public static function fromFile(string $file): array
    {
        $json = file_get_contents($file);
        if ($json === false) {
            throw new \RuntimeException("Unable to read configuration file: $file");
        }
        $config = json_decode($json, true);
        if (!is_array($config)) {
            throw new \RuntimeException('Configuration must be a JSON object');
        }
        $configs = [];
        foreach ($config as $googleSpreadsheetId => $spreadsheetConfiguration) {
            if ($googleSpreadsheetId === '$schema') {
                continue;
            }
            if (!is_string($googleSpreadsheetId) || !is_array($spreadsheetConfiguration)) {
                throw new \RuntimeException('Each spreadsheet configuration must be an object');
            }
            foreach ($spreadsheetConfiguration as $sheetName => $configuration) {
                if (!is_string($sheetName) || !is_array($configuration)) {
                    throw new \RuntimeException('Each sheet configuration must be an object');
                }
                $targetTable = $configuration['targetTable'] ?? null;
                if (!is_string($targetTable)) {
                    throw new \RuntimeException('targetTable must be a string');
                }
                $columnMapping = $configuration['columnMapping'] ?? null;
                if (!is_array($columnMapping)) {
                    throw new \RuntimeException('columnMapping must be an object');
                }
                $mapped = [];
                foreach ($columnMapping as $out => $in) {
                    if (!is_string($out) || (!is_string($in) && !is_int($in))) {
                        throw new \RuntimeException('columnMapping values must be strings or integers');
                    }
                    $mapped[$out] = $in;
                }
                $headerRow = $configuration['headerRow'] ?? 0;
                $skipRows = $configuration['skipRows'] ?? 1;
                if (!is_int($headerRow) || !is_int($skipRows)) {
                    throw new \RuntimeException('headerRow and skipRows must be integers');
                }
                $etlConfig = new EtlConfig();
                $etlConfig->googleSpreadsheetId = $googleSpreadsheetId;
                $etlConfig->sheetName = $sheetName;
                $etlConfig->targetTable = $targetTable;
                $etlConfig->columnMapping = $mapped;
                $etlConfig->headerRow = $headerRow;
                $etlConfig->skipRows = $skipRows;
                $configs[] = $etlConfig;
            }
        }
        return $configs;
    }
}
