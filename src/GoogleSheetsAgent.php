<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * Extract data from Google Sheets, load to PDO database
 */
class GoogleSheetsAgent
{
    private /* string */ $credentialsFile;
    private /* \Google_Client */ $googleClient;
    private /* float */ $loadTime;
    private /* int */ $numberOfRequestsThisSession = 0;
    
    public function __construct(string $newCredentialsFile)
    {
        json_decode(file_get_contents($newCredentialsFile)); // Validate file
        $this->credentialsFile = $newCredentialsFile;
        $this->googleClient = new \Google_Client();
        $this->googleClient->setAuthConfig($this->credentialsFile);
        $this->loadTime = microtime(true);
    }
    
    public function getAccountName()
    {
        $config = json_decode(file_get_contents($this->credentialsFile));
        return $config->client_email;
    }
    
    /**
     * List Google Sheets files chronologically by last modified date
     *
     * Files are returned if their (modification time, ID) tuple is lexically
     * greater than or equal to the given modification time and ID.
     *
     * @param string $modifiedTime limit for files to search
     * @param string $id           limit for files to search
     * @param int    $count        limit number of results
     *
     * @return array Array with elements ID -> modifiedTime (RFC 3339 format)
     *
     * @see https://developers.google.com/drive/api/v3/reference/files#list
     * @see https://tools.ietf.org/html/rfc3339
     */
    public function getOldestSpreadsheets(string $modifiedTime = '2001-01-01T12:00:00', string $id = '', int $count = 500): array
    {
        $retval = [];
        // Initialize client
        $this->googleClient->setScopes(\Google_Service_Drive::DRIVE_METADATA_READONLY);
        $googleService = new \Google_Service_Drive($this->googleClient);
        $this->throttleIfNecessary();
        
        // Collect file list
        $optParams = [
            'orderBy' => 'modifiedTime',
            'pageSize' => $count, // Google default is 100, maximum is 1000
            'q' => "mimeType = 'application/vnd.google-apps.spreadsheet' and modifiedTime >= '$modifiedTime'",
            'fields' => 'nextPageToken, files(id,modifiedTime)'
        ];
        $results = $googleService->files->listFiles($optParams);
        foreach ($results->getFiles() as $file) {
            if ($file->getModifiedTime() <= $modifiedTime) {
                if ($file->getId() < $id) {
                    continue;
                }
            }
            $retval[$file->getId()] = $file->getModifiedTime();
        }
        return $retval;
    }
    
    /**
     * Return all sheets of type GRID into a Google Spreadsheet
     *
     * @param string $spreadsheetId which spreadsheet to load
     *
     * @return array Sheet titles
     *
     * @see https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
     */
    public function getGridSheetTitles(string $spreadsheetId): array
    {
        // Initialize client
        $this->googleClient->setScopes(\Google_Service_Sheets::SPREADSHEETS_READONLY);
        $googleService = new \Google_Service_Sheets($this->googleClient);
        $this->throttleIfNecessary();
        
        // Collect file list
        $optParams = [
            'spreadsheetId' => $spreadsheetId,
            'fields' => 'sheets(properties(title,sheetType))'
        ];
        $response = $googleService->spreadsheets->get($spreadsheetId, $optParams);
        $retval = [];
        foreach ($response->getSheets() as $sheet) {
            if ($sheet->getProperties()->getSheetType() != 'GRID') {
                continue;
            }
            $retval[] = $sheet->getProperties()->getTitle();
        }
        return $retval;
    }
    
    /**
     * Load data from Google Sheets sheet as an array (rows) of arrays (columns)
     *
     * @param string $spreadsheetId  the spreadsheet load
     * @param string $sheetName      the sheet to load (must be GRID type)
     * @return RowsOfColumns
     *
     * @see https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values#ValueRange
     */
    public function getSheetRows(string $spreadsheetId, string $sheetName): RowsOfColumns
    {
        // Initialize client
        $this->googleClient->setScopes(\Google_Service_Sheets::SPREADSHEETS_READONLY);
        $googleService = new \Google_Service_Sheets($this->googleClient);
        $this->throttleIfNecessary();
        
        // Collect row data from sheet
        $response = $googleService->spreadsheets_values->get($spreadsheetId, $sheetName);
        return new RowsOfColumns($response->getValues());
    }

    /**
     * Ensures that no more than one request is sent per second
     *
     * @see https://developers.google.com/sheets/api/limits
     */
    private function throttleIfNecessary()
    {
        $secondsExecuting = microtime(true) - $this->loadTime;
        if ($this->numberOfRequestsThisSession > $secondsExecuting) {
            echo '  Throttling...' . PHP_EOL;
            usleep(($this->numberOfRequestsThisSession > $secondsExecuting) * 1000000);
        }
        $this->numberOfRequestsThisSession++;
    }
}
