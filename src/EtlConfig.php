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
    public array $columnMapping;
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
     * @param $file JSON configuration file conforming to config-schema.json
     * @return array of EtlConfig
     */
    public static function fromFile($file): array
    {
        $config = json_decode(file_get_contents($file));
        $configs = [];
        foreach ($config as $googleSpreadsheetId => $spreadsheetConfiguration) {
            if ($googleSpreadsheetId == '$schema') {
                continue;
            }
            foreach ($spreadsheetConfiguration as $sheetName => $configuration) {
                $etlConfig = new EtlConfig();
                $etlConfig->googleSpreadsheetId = $googleSpreadsheetId;
                $etlConfig->sheetName = $sheetName;
                $etlConfig->targetTable = $configuration->targetTable;
                $etlConfig->columnMapping = (array)($configuration->columnMapping);
                $etlConfig->headerRow = $configuration->headerRow ?? 0;
                $etlConfig->skipRows = $configuration->skipRows ?? 1;
                $configs[] = $etlConfig;
            }
        }
        return $configs;
    }
}