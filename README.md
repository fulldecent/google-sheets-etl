# Google Sheets ETL

[![Build and test](https://github.com/fulldecent/google-sheets-etl/actions/workflows/build-test.yml/badge.svg?branch=main)](https://github.com/fulldecent/google-sheets-etl/actions/workflows/build-test.yml) [![Lint](https://github.com/fulldecent/google-sheets-etl/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/fulldecent/google-sheets-etl/actions/workflows/lint.yml)

Import all your Google Sheets to your data warehouse, including periodic delta loads

![Screen Shot 2019-11-07 at 15 44 33](https://user-images.githubusercontent.com/382183/68426182-91f86d00-0175-11ea-8915-3ca3700488bd.png)

See `example.php` for how to use this library.

## Installation

You will need PHP 8.3 or newer and Composer 2.

```sh
composer require fulldecent/google-sheets-etl
```

That installs the latest stable release from Packagist. Packagist publishes each `v` tag.

Next, create a Google Service Account. This requires 20 steps so we made a [step-by-step illustrated guide](GOOGLE-SETUP.md).

## Development

Thank you for taking an interest in improving Google Sheets ETL.

CI installs the PHP version in [.php-version](.php-version). That file says 8.3, which is the oldest PHP allowed by [composer.json](composer.json). `config.platform.php` is `8.3.99`, so `composer update` on a newer PHP does not lock a dependency that requires 8.4.

```sh
composer install --no-interaction --prefer-dist
composer test
composer check
```

`composer test` runs PHPUnit. `composer check` runs PHP-CS-Fixer in dry-run mode and PHPStan at level 9. Apply PHP-CS-Fixer when it reports a diff:

```sh
vendor/bin/php-cs-fixer fix
```

With an actively maintained version of Node.js installed, correct other formatting issues before sending proposed changes:

```sh
npx prettier@latest --check . --write
npx markdownlint-cli@latest "**/*.md" --fix
```

## Releases

Versions through 1.0.3 were tagged `1.0.3`. Release Please reads a tag shaped like `v1.0.3`. That tag points at the same commit as `1.0.3`. Composer and Packagist treat both as version 1.0.3.

Use `fix:`, `feat:` or `BREAKING CHANGE:` in your commit messages. This triggers our bot to make a release draft pull request. Merging that pull request triggers a new tag and GitHub Release. A commit with any other prefix does not open that pull request.

The [release workflow](.github/workflows/release.yml) uses Release Please's `simple` release type. It does not add a `version` field to [composer.json](composer.json). The tag is the version. The published zip is `google-sheets-etl.zip`, named from the Composer package, plus `release.sigstore.jsonl`.

Packagist is already connected to this repository. The publish job's tag is what `composer require fulldecent/google-sheets-etl` installs.

## Google Sheets limitations

We found several problems with using Google Sheets as a database, even though we continue to use it:

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
- Filters cannot be used, because they hide rows for everybody
  - If using another mode "filter views", which is harder to find, it will create hundreds of saved "Filter 1", "Filter 2" ... files.

## Maintenance

The project administrator completes these maintenance tasks each month. If they are 3+ months late, please remind them or send your own issue/pull request.

1. Identify external Actions in [.github/workflows](.github/workflows) and look for available new versions. Review and update them if it is safe. GitHub-supported Actions (under the actions/ organization) may require only cursory review. [shivammathur/setup-php](https://github.com/shivammathur/setup-php) and [googleapis/release-please-action](https://github.com/googleapis/release-please-action) are not in that organization, so read their changelogs.
1. Review the PHP version in [.php-version](.php-version) and the `php` constraint in [composer.json](composer.json). Drop a PHP version when it no longer receives security fixes. Keep those two lines on the same version.
1. Review direct dependencies with `composer outdated --direct` and `composer audit`.

## References

1. This project is built based on [best practices documented in php-package-template](https://github.com/fulldecent/php-package-template/), release 1.0.0.
1. PHP project layout from [thephpleague/skeleton](https://github.com/thephpleague/skeleton).
1. "You should never catch errors to report them" [https://phpdelusions.net/pdo#errors](https://phpdelusions.net/pdo#errors)
