# Creating ETL for BigQuery

## Generating Keys from GCP
Making Service Account on Google Cloud Platform:
  1. Hit the main menu on the top left corner and navigate to `IAM & Admin`
  2. Click `Service Accounts` in the `IAM & Admin` submenu
  3. In the Service Accounts menu on top, click `CREATE SERVICE ACCOUNT`
  4. Fill out the information
      - Service account name: Name of the service account
      - Service account ID: Automatically generated after you create the name
      - Service account description: Description of the service account
     Then click `CREATE AND CONTINUE`
  5. Grant the access to project on the Service Account you just created
      These access are based on the BigQuery ETL project
      - BigQuery Admin
      - BigQuery User
      - BigQuery Connection Service Agent
  6. You can skip the last configuration part for this project

Creating new Key for Service Account
  1. Click the ellipsis on the right side of the Service Account you are going to use
  2. Click `Manage keys` on the drop-down menu
  3. In the Keys page, click `ADD KEY` then click `Create new key`
  4. There are two options:
      - JSON (Recommended)
      - P12 (For backward compatibility with code using the P12 format)
     Click `JSON`
  5. After the creation, the json key will automatically start downloading to your machine

