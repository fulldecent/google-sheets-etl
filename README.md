[![Latest Version on Packagist](https://img.shields.io/packagist/v/fulldecent/google-sheets-etl.svg?style=flat-square)](https://packagist.org/packages/fulldecent/google-sheets-etl)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)](LICENSE.md)
[![Build Status](https://img.shields.io/travis/fulldecent/google-sheets-etl/master.svg?style=flat-square)](https://travis-ci.org/fulldecent/google-sheets-etl)
[![Coverage Status](https://img.shields.io/scrutinizer/coverage/g/fulldecent/google-sheets-etl.svg?style=flat-square)](https://scrutinizer-ci.com/g/fulldecent/google-sheets-etl/code-structure)
[![Quality Score](https://img.shields.io/scrutinizer/g/fulldecent/google-sheets-etl.svg?style=flat-square)](https://scrutinizer-ci.com/g/fulldecent/google-sheets-etl)
[![Total Downloads](https://img.shields.io/packagist/dt/fulldecent/google-sheets-etl.svg?style=flat-square)](https://packagist.org/packages/fulldecent/google-sheets-etl)

Google Sheets ETL
=================

Live import all your Google Sheets to your data warehouse


<img width="1993" alt="Screen Shot 2019-11-07 at 15 44 33" src="https://user-images.githubusercontent.com/382183/68426182-91f86d00-0175-11ea-8915-3ca3700488bd.png">


See `example.php` how to use this library.

## Install

Via Composer

```sh
composer install
```

Next, create a Google Service Account. This requires 20 steps so we made a [a step-by-step illustrated guide](GOOGLE-SETUP.md).

## Testing

```sh
composer test
```

## Google Sheets Limitations

We have found several problems with using Google Sheets as a database, even though we will continue to use it:

- Cannot restrict editing the first row (headers) to certain people
  - If you try protecting the cells it will prevent everyone from using a filter which is unacceptable
  - Sometimes the page will load slowly and your collaborators will accidentally overwrite the first row, which is default-selected, and it will cause your ETL to error until fixed
- Cannot restrict that any formatting must apply to the entire column (including new rows)
  - Inevitably, any conditional formatting you try to set up will apply to a disjoint set of cells throughout your sheet over time
- Cannot restrict that formulas must apply to the entire column (including new rows)
  - Inevitably, over time your calculated "status" column will turn into the text literal "DONE" as people copy-paste-values to new rows
- Cannot limit people from using formatting in cells (which comes by default when they paste into cells)
- Cannot enforce a unique column
  - Creating a custom data validation formula is cumbersome and not reliable, plus other collaborators can defeat it
- Cannot create a sheet-level comment to document the purpose of the whole sheet 
- Filters cannot be used, because by default they will prevent all other workers from seeking rows that they want
  - If using another mode "filter views", which is harder to find, it will create hundreds of saved "Filter 1", "Filter 2" ... files.

## References

* PHP project layout from https://github.com/thephpleague/skeleton
* "You should never catch errors to report them" https://phpdelusions.net/pdo#errors

