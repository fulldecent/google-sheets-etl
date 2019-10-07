Google Service Account Setup
=================

You will create a Google Service Account which you, your employees, or others will share files with. Google Sheets ETL will login to Google as the service account to access those files.

## Create a Google account

Use your Gmail account or corporate account.

## Create Cloud Platform project

Start at https://console.cloud.google.com/

![1 create project](GOOGLE-SETUP.assets/1 create project.png)

![2 set project name](GOOGLE-SETUP.assets/2 set project name.png)

Then you have to open that project (it takes a moment to create).

![3 open that project](GOOGLE-SETUP.assets/3 open that project.png)

## Add the Google Drive API to the project

![4 access api settings](GOOGLE-SETUP.assets/4 access api settings.png)

![5 enable api](GOOGLE-SETUP.assets/5 enable api.png)

![6 select drive api](GOOGLE-SETUP.assets/6 select drive api.png)

![7 enable drive api](GOOGLE-SETUP.assets/7 enable drive api.png)

## Add the Google Sheets API to the project

![8 select project](GOOGLE-SETUP.assets/8 select project.png)

![9 access api settings](GOOGLE-SETUP.assets/9 access api settings.png)

![10 enable api](GOOGLE-SETUP.assets/10 enable api.png)

![11 select sheets api](GOOGLE-SETUP.assets/11 select sheets api.png)

![12 enable sheets api](GOOGLE-SETUP.assets/12 enable sheets api.png)

## Create a service account for the project

![13 create credentials](GOOGLE-SETUP.assets/13 create credentials.png)

![14 select service accounts](GOOGLE-SETUP.assets/14 select service accounts.png)

![15 create service account](GOOGLE-SETUP.assets/15 create service account.png)

![16 configure service account](GOOGLE-SETUP.assets/16 configure service account.png)

![17 skip permissions](GOOGLE-SETUP.assets/17 skip permissions.png)

## Create an access key for this service account

![18 create key](GOOGLE-SETUP.assets/18 create key.png)

![19 download key](GOOGLE-SETUP.assets/19 download key.png)

## Open the key

![20 view key](GOOGLE-SETUP.assets/20 view key.png)

The client_email is the email address for your service account. You will share your Google Sheets with that account, or entire folders from Google Drive.