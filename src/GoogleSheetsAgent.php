<?php
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
    
    function __construct(string $newCredentialsFile)
    {
        json_decode(file_get_contents($newCredentialsFile)); // Validate that file decodes
        $this->credentialsFile = $newCredentialsFile;
        $this->googleClient = new \Google_Client();    
        $this->googleClient->setAuthConfig($this->credentialsFile);
        $this->loadTime = microtime(true);
    }
    
    function getAccountName()
    {
        $config = json_decode(file_get_contents($this->credentialsFile));
        return $config->client_email;
    }
    
    /**
     * List Google Sheets files chronological by last modified date
     *
     * Files are returned if their (modification time, ID) tuple is lexically
     * greater than the giver modification time and ID.
     * 
     * @see https://developers.google.com/drive/api/v3/reference/files#list
     * @see https://tools.ietf.org/html/rfc3339
     * 
     * @param string $modifiedAfter
     * @param string $idGreaterThan
     * @param int $count Limit number of results
     * @return ?array Array with elements ID -> modifiedTime (RFC 3339 format)
     */
    function getOldestSpreadsheets(string $modifiedAfter='2001-01-01T12:00:00', string $idGreaterThan='', int $count=500): array
    {
        $this->assertValidRfc3339Date($modifiedAfter);
        $retval = [];
        // Initialize client
        $this->googleClient->setScopes(\Google_Service_Drive::DRIVE_METADATA_READONLY);
        $googleService = new \Google_Service_Drive($this->googleClient);
        $this->throttleIfNecessary();
        
        // Collect file list
        $optParams = [
            'orderBy' => 'modifiedTime',
            'pageSize' => $count, # Google default is 100, maximum is 1000
            'q' => "mimeType = 'application/vnd.google-apps.spreadsheet' and modifiedTime >= '$modifiedAfter'",
            'fields' => 'nextPageToken, files(id,modifiedTime)'
        ];            
        $results = $googleService->files->listFiles($optParams);
        foreach ($results->getFiles() as $file) {
            if ($file->getModifiedTime() <= $modifiedAfter) {
                if ($file->getId() <= $idGreaterThan) {
                    continue;
                }
            }
            $retval[$file->getId()] = $file->getModifiedTime();
        }
        return $retval;
    }
    
    /**
     * Return all sheets of type GRID in a Google Spreadsheet
     *
     * @see https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
     * 
     * @param string $spreadsheetId
     * @return array Sheet titles
     */
    function getGridSheetTitles(string $spreadsheetId): array
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
        foreach($response->getSheets() as $sheet) {
            if ($sheet->getProperties()->getSheetType() != 'GRID') continue;
            $retval[] = $sheet->getProperties()->getTitle();
        }
        return $retval;
    }  
    
    /**
     * Load data from Google Sheets sheet as array (rows) of arrays (columns)
     *
     * @see https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values#ValueRange
     * @param string $spreadsheetId The spreadsheet load
     * @param string $sheetName The sheet to load (must be GRID type)
     * @return array
     */
    function getSheetRows(string $spreadsheetId, string $sheetName): RowsOfColumns
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

    private function assertValidRfc3339Date(string $date) {
        /*
        PHP validation of RFC3999 is broken
        $retult = preg_match('/^([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?(([Zz])|([\+|\-]([01][0-9]|2[0-3]):[0-5][0-9]))$/', $date);

        if (!$result) {
            echo 'DATE IS NOT VALID RFC 3339' . PHP_EOL;
            echo $date;
            die();
        }
        */
    }
}
