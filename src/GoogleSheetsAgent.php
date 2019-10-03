<?php
namespace fulldecent\GoogleSheetsEtl;

/**
* Extract data from Google Sheets, load to PDO database
*/
class GoogleSheetsAgent
{
    private /* string */ $credentialsFile;
    private /* ?string */ $loadTime;
    private /* \Google_Client */ $googleClient;
    
    function __construct(string $newCredentialsFile)
    {
        json_decode(file_get_contents($newCredentialsFile)); // Validate that file successfully decodes
        $this->credentialsFile = $newCredentialsFile;
        $this->loadTime = date('Y-m-d H:i:s');
        $this->googleClient = new \Google_Client();    
        $this->googleClient->setAuthConfig($this->credentialsFile);
    }
    
    function getAccountName()
    {
        $config = json_decode(file_get_contents($this->credentialsFile));
        return $config->client_email;
    }
    
    /**
     * List Google Sheets files. Chronological by last modified date.
     *
     * @see https://developers.google.com/drive/api/v3/reference/files#list
     * @see https://tools.ietf.org/html/rfc3339
     * @todo The since is insufficient to resume progress because two files could
     *       be modified at the same second. Improve this by ordering
     *       deterministicly by modifiedTime and then id and also have a "since"
     *       parameter for the id.
     * 
     * @param string $credentialsFile
     * @param string $since Limit results to modified since then
     * @param int $count Limit number of results
     * @return ?array Array with elements ID -> modifiedTime (RFC 3339 format)
     */
    function listSomeSpreadsheets(string $since='2001-01-01T12:00:00', int $count=500): ?array
    {
        // Initialize client
        $this->googleClient->setScopes(\Google_Service_Drive::DRIVE_METADATA_READONLY);
        $googleService = new \Google_Service_Drive($this->googleClient);
        
        // Collect file list
        try {
            echo "TODO: switch this from >= to > after 'do not select a row if one sheet was not loaded'\n";      
            $optParams = [
                'orderBy' => 'modifiedTime',
                'pageSize' => $count, # Google default is 100, maximum is 1000
                'q' => "mimeType = 'application/vnd.google-apps.spreadsheet' and modifiedTime >= '$since'",
                'fields' => 'nextPageToken, files(id,modifiedTime)'
            ];
            
            $results = $googleService->files->listFiles($optParams);
            
            if (count($results->getFiles()) == 0) {
                print "No files found.\n";
                return null;
            }
            $retval = [];
            foreach ($results->getFiles() as $file) {
                $retval[$file->getId()] = $file->getModifiedTime();
            }
            return $retval;
        } catch (\Google_Service_Exception $e) {
            echo 'ERROR GOOGLE SERVICE: ';
            echo json_encode($e->getErrors());
            return null;
        }
    }
    
    /**
     * Truncate/pad all arrays in row to a given length
     *
     * @param array $rows
     * @param integer $length
     * @return array array containing arrays each with size LENGTH
     */
    private function normalizeArraysToLength(array $rows, int $length): array
    {
        $retval = [];
        foreach ($rows as $row) {
            $row = array_slice($row, 0, $length);
            $row = array_pad($row, $length, null);
            $retval[] = $row;
        }
        return $retval;
    }
    
    /**
     * Return all sheets of type GRID in a Google Spreadsheet
     *
     * @param string $spreadsheetId
     * @return array Sheet titles
     */
    function getGridSheetTitles(string $spreadsheetId): array
    {
        // Initialize client
        $this->googleClient->setScopes(\Google_Service_Sheets::SPREADSHEETS_READONLY);
        $googleService = new \Google_Service_Sheets($this->googleClient);
        
        // Collect file list
        try {
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
        } catch (\Google_Service_Exception $e) {
            echo 'ERROR: ';
            echo json_encode($e->getErrors());
            die();
        }
    }  
    
    /**
     * Load data from Google Sheets sheet as array (rows) of arrays (columns)
     *
     * @param string $spreadsheetId The spreadsheet load
     * @param string $sheetName The sheet to load (must be GRID type)
     * @return array
     */
    function getSheetRows(string $spreadsheetId, string $sheetName): array
    {
        // Initialize client
        $this->googleClient->setScopes(\Google_Service_Sheets::SPREADSHEETS_READONLY);
        $googleService = new \Google_Service_Sheets($this->googleClient);
        
        // Collect row data from sheet
        try {
            $response = $googleService->spreadsheets_values->get($spreadsheetId, $sheetName);
            $sheetRows = $response->getValues();
            return $sheetRows ?? [];
        } catch (\Google_Service_Exception $e) {
            echo 'ERROR: ';
            echo json_encode($e->getErrors());
            die();
        }
    }
}
