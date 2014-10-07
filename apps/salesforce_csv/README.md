# salesforce_csv
Downloads data from Salesforce to a local CSV. 

## Features
* Uses Salesforce Bulk API for downloading data, which is faster for big data volumes than REST and SOAP.
* For SFDC objects that aren't supported on Bulk API, uses REST API
* By default, downloads all fields on given objects.

## Credentials
See [credentials.json](config/credentials.json)

You'll need a Salesforce OAuth application, this identifies you as a developer accessing Salesforce. See [the official manual](http://www.salesforce.com/us/developer/docs/api_rest/Content/quickstart_oauth.htm#step1_oauth) on how to get the id and secret. To create an OAuth app you need a salesforce account, e.g. a [developer edition account](https://developer.salesforce.com/signup).

For playing around you can use mine. client id: `3MVG9QDx8IX8nP5TCIkVJaz5TTjysV5jnBKX2FluOu0U9Z_Y8mWTHsTlKywFUKYsPpQWWNWjg5yFsxmBC.0nj` client secret: `931268898124987988`.