<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * A structure which holds an array (rows) of arrays (columns) containing data.
 *
 * Empty trailing rows and columns will not be included.
 * @reference https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values#ValueRange
 */
class RowsOfColumns
{
    private /* array */ $rowsOfColumns;

    function __construct(array $values)
    {
        $this->rowsOfColumns = $values;
    }

    /**
     * Gets column selectors for the specified names from a header row
     *
     * @param array   $specifiers  indicies (zero-indexed) or names of columns
     * @param integer $headerRow   which row (zero-indexed) to retrieve from
     * @return array               columns (zero-indexed) for names specified
     */
    function getColumnSelectorsFromHeaderRow(array $specifiers, int $headerRow = 0): array
    {
        $retval = [];
        $row = $this->rowsOfColumns[$headerRow];
        foreach ($specifiers as $specifier) {
            if (is_int($specifier)) {
                $retval[] = $specifier;
            } elseif (is_string($specifier)) {
                $selector = array_search($specifier, $row, true);
                if ($selector === false) {
                    throw new \Exception('Required column not found: ' . $specifier);
                }
                $retval[] = $selector;
            }
        }
        return $retval;
    }

    /**
     * Return a two-dimensional array of values
     *
     * @param array   $columnSelectors  specifyies which columns (zero-indexed)
     *                                  to extract from each row of source data
     * @param integer $skipRows         number of rows to skip from source data
     * @return array                    rows, each containing an array (columns)
     *                                  of values
     */
    function getRows(array $columnSelectors, int $skipRows = 1): array
    {
        $retval = [];
        foreach (array_slice($this->rowsOfColumns, $skipRows) as $row) {
            $retvalRow = [];
            foreach ($columnSelectors as $columnSelector) {
                $retvalRow[] = $row[$columnSelector] ?? null;
            }
            $retval[] = $retvalRow;
        }
        return $retval;
    }
}