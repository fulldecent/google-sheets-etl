# Google Sheets ETL

[![Build and test](https://github.com/fulldecent/google-sheets-etl/actions/workflows/build-test.yml/badge.svg)](https://github.com/fulldecent/google-sheets-etl/actions/workflows/build-test.yml) [![Lint](https://github.com/fulldecent/google-sheets-etl/actions/workflows/lint.yml/badge.svg)](https://github.com/fulldecent/google-sheets-etl/actions/workflows/lint.yml)

Google Sheets ETL copies configured sheets into MySQL or SQLite. A later run loads a sheet again when that spreadsheet's modified time has changed.

![Sheets loaded into a database](https://user-images.githubusercontent.com/382183/68426182-91f86d00-0175-11ea-8915-3ca3700488bd.png)

```sh
php bin/google-sheets-etl credentials.json etl.json "sqlite:warehouse.db"
```

## Installation

You will need Git, PHP at the version in [.php-version](.php-version), Composer 2, and the PHP extensions `curl`, `iconv`, `json`, `openssl`, `pdo`, `dom`, `mbstring`, `tokenizer`, `xml`, `xmlwriter` and `zip`. Loading a database also needs `pdo_mysql` or `pdo_sqlite`. The test suite uses `pdo_sqlite`. Open a terminal (PowerShell on Windows) and use the instructions for your operating system.

The PHP version below is 8.4 because that is the oldest branch in active support, and that is what [.php-version](.php-version) says. When that file changes, change these commands with it. CI runs the test job on every branch in the matrix in [build-test.yml](.github/workflows/build-test.yml). The pack job installs the version in that file with [shivammathur/setup-php](https://github.com/shivammathur/setup-php). A distribution package named `php` that ignores the file is the wrong PHP for local setup.

> [!WARNING]
> Do not install PHP or Composer by piping a downloaded script into a shell. The commands below use a package manager, or a named archive for the current PHP packages.

### Linux

On Ubuntu 22.04+ or Debian 12+, install PHP 8.4 from the [Ondřej Surý PHP archive](https://launchpad.net/~ondrej/+archive/ubuntu/php):

```sh
sudo apt update
sudo apt install -y ca-certificates software-properties-common
sudo add-apt-repository -y ppa:ondrej/php
sudo apt update
sudo apt install -y git php8.4-cli php8.4-xml php8.4-mbstring php8.4-zip php8.4-curl php8.4-sqlite3 php8.4-mysql unzip composer
sudo update-alternatives --set php /usr/bin/php8.4
```

`php8.4-xml` provides `dom`, `tokenizer`, `xml` and `xmlwriter`. `php8.4-sqlite3` provides `pdo_sqlite`. `php8.4-mysql` provides `pdo_mysql`.

On Fedora, install PHP 8.4 from the [Remi repository](https://rpms.remirepo.net/wizard/):

```sh
sudo dnf install -y https://rpms.remirepo.net/fedora/remi-release-$(rpm -E %fedora).rpm
sudo dnf module reset -y php
sudo dnf module enable -y php:remi-8.4
sudo dnf install -y git php-cli php-xml php-mbstring php-zip php-pdo php-mysqlnd php-sqlite3 composer
```

### macOS

Install Apple's Command Line Tools if they are not already installed:

```sh
xcode-select --install
```

Complete the installation dialog. With Homebrew installed, install Git, PHP 8.4 and Composer:

```sh
brew install git php@8.4 composer
brew link --overwrite --force php@8.4
```

[php@8.4](https://formulae.brew.sh/formula/php@8.4) stays on PHP 8.4. Homebrew's unversioned `php` formula moves to the next release on its own, so it will not keep matching [.php-version](.php-version). `brew link` is what puts that `php` on your `PATH`.

### Windows

Use winget to install Git, the non-thread-safe PHP 8.4 build (the CLI build) and Composer. Open a new PowerShell window after installation so `PATH` is current.

```powershell
winget install --exact --id Git.Git
winget install --exact --id PHP.PHP.NTS.8.4
winget install --exact --id Composer.Composer
```

### Build and install

Create a Google service account and share each spreadsheet with it. The illustrated steps are in [GOOGLE-SETUP.md](GOOGLE-SETUP.md).

Install the package in another project:

```sh
composer require fulldecent/google-sheets-etl
```

Packagist installs the version from the git tag. `composer require fulldecent/google-sheets-etl:dev-main` installs the default branch instead.

To work on this repository, clone it and install dependencies. `composer install` uses [composer.lock](composer.lock).

```sh
git clone https://github.com/fulldecent/google-sheets-etl.git
cd google-sheets-etl
php --version
composer --version
composer install --no-interaction --prefer-dist
```

Run the command without a global install:

```sh
php bin/google-sheets-etl credentials.json etl.json "sqlite:warehouse.db"
```

`composer install` puts the `google-sheets-etl` command on `vendor/bin/google-sheets-etl` as well.

## Usage

The command takes a service-account JSON key, an ETL schema, and a PDO DSN. A username and password are optional and are used for MySQL.

```sh
php bin/google-sheets-etl GOOGLE_KEY_JSON ETL_SCHEMA_JSON DATABASE_DSN [DATABASE_USERNAME [DATABASE_PASS]]
```

`DATABASE_DSN` is a PDO DSN. `sqlite:warehouse.db` uses SQLite. `mysql:host=127.0.0.1;dbname=warehouse` uses MySQL. Other drivers are rejected.

The schema is a JSON object that conforms to [config-schema.json](config-schema.json). Each key except `$schema` is a spreadsheet id. Each sheet entry names the destination table and maps destination columns to a header label or a zero-indexed column number. `headerRow` defaults to 0. `skipRows` defaults to 1.

```json
{
  "$schema": "./config-schema.json",
  "1b33RL2nQJxdaHYxVmkk4lo3K1IKjSD3_ggnokrZCkx8": {
    "2019 Expirations": {
      "targetTable": "certification-course-renewals-2019",
      "columnMapping": {"out1": "in1", "out2": 2},
      "headerRow": 0,
      "skipRows": 1
    }
  }
}
```

From another project that requires this package:

```php
use fulldecent\GoogleSheetsEtl\Tasks;

$database = new PDO($dsn, $user, $password, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);
$tasks = new Tasks($credentialsFile, $database);
$tasks->loadConfiguration($schemaFile);
$tasks->databaseAgent->setUpAccounting();
$tasks->loadSomeUpdatedSpreadsheets();
```

`findSomeUpdatedSpreadsheets` records up to 200 spreadsheets the service account can see, in order of modified time, starting after the last one already recorded. `loadSomeUpdatedSpreadsheets` reads each configured sheet that is already recorded and whose modified time is not already loaded. When the sheet contents have changed, it deletes that sheet's rows and inserts the new ones. A load of a spreadsheet that has not been recorded throws `Spreadsheet not found`. Missing columns throw an exception that names the spreadsheet and the sheet.

The program writes progress to standard output. A missing argument prints usage and exits. It does not edit the spreadsheets.

## Development

Thank you for taking an interest in improving Google Sheets ETL and the programs of people using it!

Follow the installation instructions above to get Git, PHP 8.4 and Composer. Work from the project directory. The implementation is in [src/](src/) and [bin/google-sheets-etl](bin/google-sheets-etl). You can run it without a global install:

```sh
php bin/google-sheets-etl credentials.json etl.json "sqlite:warehouse.db"
```

Commit [composer.lock](composer.lock) so the development tools stay reproducible. The published archive omits that file. People who require the library resolve their own dependencies from [composer.json](composer.json).

There is no `version` field in [composer.json](composer.json). Packagist and the release workflow take the version from the git tag. A `version` field in the manifest fights the tag.

[composer.json](composer.json) requires `>=8.4 <8.6` and sets `config.platform.php` to `8.4.99`. `8.4.99` is the newest patch Composer will assume for the oldest supported branch. A checkout can run a newer PHP than that branch. Without the pin, `composer update` can lock a dependency that requires the newer PHP, and the 8.4 test job fails when it installs the lock.

### Testing

All project updates that we release must conform to our test suite. GitHub Actions runs [checks](./.github/workflows) on pushes to `main` and pull requests. You can also run them locally before sending proposed changes:

```sh
composer test
composer check
```

`composer test` runs PHPUnit. `composer check` runs PHP-CS-Fixer in dry-run mode and PHPStan at level 9. The tests in [tests/](tests/) cover spreadsheet accounting, SQLite loads, the Google Sheets client, and the task flow.

Apply PHP-CS-Fixer when it reports a diff:

```sh
vendor/bin/php-cs-fixer fix
```

With an actively maintained version of Node.js installed, correct other formatting issues before sending proposed changes:

```sh
npx prettier@latest --check . --write
npx markdownlint-cli@latest "**/*.md" --fix
```

Composer puts install outputs in the ignored `vendor/` directory. Packed releases go in the ignored `dist/` directory. Service-account keys and local databases stay in the ignored `local/` directory. See [local/README.md](local/README.md).

### Releases

Use `fix:`, `feat:` or `BREAKING CHANGE:` in your commit messages. This triggers our bot to make a release draft pull request. Merging that pull request triggers a new tag and GitHub Release.

The [release workflow](.github/workflows/release.yml) uses Release Please's `simple` release type. It does not need a release-please config file. Do not add a `version` field to [composer.json](composer.json) in the release pull request. The tag is the version.

A commit on `main` whose message starts with `fix:` opens or updates a release pull request that bumps the patch version. `feat:` bumps the minor version. `BREAKING CHANGE:` bumps the major version. One release pull request collects every such commit since the last `v` tag. A commit with any other prefix, including `chore:` and `docs:`, does not open that pull request. Merging the release pull request is the release. Release Please writes [CHANGELOG.md](CHANGELOG.md) in that pull request and does not create the tag. The publish job creates the tag.

[Build and test](.github/workflows/build-test.yml) installs dependencies, checks style and types, runs tests, packs the zip, then attests and uploads it. The zip name comes from the Composer package name, the part after the slash. This package is `fulldecent/google-sheets-etl`, so the release includes `google-sheets-etl.zip` and `release.sigstore.jsonl`, containing build provenance and version attestations. The zip filename has no version in it. The version is the tag, and the attestation records that version without a leading `v`.

Packagist installs this library from the git tag. The GitHub hook is already connected ([how to update packages](https://packagist.org/about#how-to-update-packages)). Each tag the publish job pushes then becomes a Packagist version. `composer require fulldecent/google-sheets-etl` installs that version. The workflow does not send a Packagist token. Packagist does not serve the attested zip. The zip is the GitHub Release asset.

Release Please continues from the latest tag shaped like `v1.2.3`. Composer and Packagist treat that tag as version 1.2.3. A tag shaped like `1.2.3` does not count.

The release workflow needs repository settings under Actions, General, Workflow permissions: read and write permissions, and "Allow GitHub Actions to create and approve pull requests". Under General, Releases, release immutability must be enabled. Attestations are available for public repositories; private repositories require GitHub Enterprise Cloud.

### Maintenance

The project administrator completes these maintenance tasks each month. If they are 3+ months late, please remind them or send your own issue/pull request.

1. Identify external Actions in [.github/workflows](.github/workflows) and look for available new versions. Review and update them if it is safe. GitHub-supported Actions (under the actions/ organization) may require only cursory review. [shivammathur/setup-php](https://github.com/shivammathur/setup-php) and [googleapis/release-please-action](https://github.com/googleapis/release-please-action) are not in that organization, so read their changelogs.
1. Review which PHP branches are in active support on the [supported versions](https://www.php.net/supported-versions.php) page. A branch in security support only, or past its end of life, is not supported. Each branch that is in active support must pass the test job. The `php` matrix in [build-test.yml](.github/workflows/build-test.yml) is that list.

   To change the set:

   1. Set `require.php` in [composer.json](composer.json) to `>=X.Y <Z.W`. `X.Y` is the oldest branch still in active support. `Z.W` is the next branch after the newest one in active support, so a branch that is not yet in active support is rejected.
   1. Set `config.platform.php` to `X.Y.99` and set [.php-version](.php-version) to `X.Y`. `X.Y` in that file means the newest patch of that branch.
   1. Set `matrix.php` in [build-test.yml](.github/workflows/build-test.yml) to every branch in active support. Leave the pack job id as `build-test` and leave the uploaded artifact named `build`. The release workflow downloads that artifact.
   1. Change the installation commands in this file to `X.Y`.
   1. Run `composer update` so [composer.lock](composer.lock) matches the platform pin. `composer check` and `composer test` must pass on each version in the matrix before the change is merged.
1. Review direct dependencies with `composer outdated --direct`.

## Project scope

We are people who keep working records in Google Sheets and need those rows in a SQL database.

Google Sheets ETL does that job. It reads sheets shared with one Google service account, loads the configured columns into MySQL or SQLite, and records the spreadsheet modified time so an unchanged sheet is not loaded again.

We specifically will not edit spreadsheets, and we will not add a way to make Google Sheets enforce headers, unique columns, or column-wide formulas. Sheets still allows a collaborator to overwrite the header row, to paste formatting into one cell, or to turn a formula into the text `DONE`. Filters hide rows for everybody. Those limits stay in Google Sheets. This project also will not add databases other than MySQL and SQLite, or a graphical interface.

## References

1. We use title case only for proper nouns, including the name of our project.
1. [PSR-12](https://www.php-fig.org/psr/psr-12/) is why `*.php` uses four spaces in [.editorconfig](.editorconfig). Other files do not set an indent size.
1. [composer.json `version`](https://getcomposer.org/doc/04-schema.md#version) is omitted. The git tag is the version. [composer.lock](composer.lock) is committed so CI is reproducible ([Composer: commit your lock file](https://getcomposer.org/doc/01-basic-usage.md#commit-your-composer-lock-file-to-version-control)); a library is allowed to leave it uncommitted ([Composer: lock file for libraries](https://getcomposer.org/doc/02-libraries.md#lock-file)), and the published archive does leave it out.
1. The command opens PDO with exceptions enabled. "You should never catch errors to report them" is the reason that mode stays on. [PDO](https://phpdelusions.net/pdo#errors)
1. This project is built based on [best practices documented in php-package-template](https://github.com/fulldecent/php-package-template/), commit [c84aa458](https://github.com/fulldecent/php-package-template/commit/c84aa4580d0e175da189457711ace70c5e4b6cfb). The default branch is past release v1.1.0, so this cites that commit.
1. The floor for the README, the lint workflow and the release workflow comes from [project-template](https://github.com/fulldecent/project-template), release v1.4.1. The release attestation shape comes from [node.js-template](https://github.com/fulldecent/node.js-template/), release 1.0.0.
1. The Composer ignore rules in [.gitignore](.gitignore) come from [GitHub's Composer gitignore](https://github.com/github/gitignore/blob/main/Composer.gitignore).
1. This project is released under the [MIT license](LICENSE.md).
