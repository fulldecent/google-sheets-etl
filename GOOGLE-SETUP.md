# Google service account setup

You will create a Google Service Account which you, your employees, or others will share files with. Google Sheets ETL will log into Google as this service account to access those files.

## Create a Google account

Use your Gmail account or corporate account.

## Create a Cloud Platform project

1. Start at https://console.cloud.google.com/ and create a project
   ![1 create project](./GOOGLE-SETUP.assets/1 create project.png)

2. Open that project (it takes a moment to load)
   ![2 open project](./GOOGLE-SETUP.assets/2 open project.png)

3. Add the Google Drive API to the project
   ![3 add google drive api A](./GOOGLE-SETUP.assets/3 add google drive api A.png)

   ![3 add google drive api B](./GOOGLE-SETUP.assets/3 add google drive api B.png)

   ![3 add google drive api C](./GOOGLE-SETUP.assets/3 add google drive api C.png)

   ![3 add google drive api D](./GOOGLE-SETUP.assets/3 add google drive api D.png)

   ![3 add google drive api E](./GOOGLE-SETUP.assets/3 add google drive api E.png)

4. Add the Google Sheets API to the project

   1. Click GOOGLE CLOUD logo on the top left
   2. Repeat the step above to OPEN the project
   3. Repeat the step above to ENABLE API
   4. Repeat the step above to ENABLE the Google Sheets API (instead of the Google Drive API)

5. Create a service account for the project

   ![5 create service account A](./GOOGLE-SETUP.assets/5 create service account A.png)

   ![5 create service account B](./GOOGLE-SETUP.assets/5 create service account B.png)

   ![5 create service account C](./GOOGLE-SETUP.assets/5 create service account C.png)

   ![5 create service account D](./GOOGLE-SETUP.assets/5 create service account D.png)

6. Create key

   ![6 create key A](./GOOGLE-SETUP.assets/6 create key A.png)

   ![6 create key B](./GOOGLE-SETUP.assets/6 create key B.png)

7. Open that downloaded file

   1. The `client_email` is the email address for your service account. You will share your Google Sheets with that account, or entire folders from Google Drive.
   2. Save that key file to your `local` folder in the project folder.