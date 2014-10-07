# dss_csv
Unloads data from ADS to local CSVs, using generated or user-defined CSVs.

## Features
* Running user-defined SQL on ADS and storing results to a local CSV
* Generating SQL for simple queries (with no joins), taking the last snapshot

## Generating SQLs
Configuration has to be provided for each query. 

### Simple column-to-column
If there's no transformation and a SQL column goes directly to a CSV column, just provide the CSV column name as a key and SQL column as `source_column`. Example:
```
{
  "dataset_mapping": {
    "salesforce": {
      "account":{
        "source_object":"Account",
        "columns":{
          "id":{
            "source_column":"Id"
          },
          "type":{
            "source_column":"Type",
            "optional":true
          },
          ...
```
This will produce a file `account.csv` with column `id` coming from the `Id` column of the `Acccount` object stored in ADS. If the column is marked as `optional` it doesn't have to exist in the table. If it doesn't exist, it's filled with an empty value. 

### Concatenation
A CSV column values can be concated from multiple SQL columns and metadata, using `source_column_concat`. Metadata values start with a colon.
Example:
```
{
  "dataset_mapping": {
    "salesforce": {
      "account":{
        "source_object":"Account",
        "columns":{
          "url":{
            "source_column_concat":[
              ":salesforce_server",
              "Id"
            ]
          }
          ...
```
The value in the `url` CSV column will be obtained by concatening the `salesforce_server` metadata value (stored in meta table) and the `Id` SQL column. 

### SQL expression
In more difficult cases you can use an SQL expression that will be outputted to the CSV column, by providing `source_column_expression`. Example:
```
"dataset_mapping": {
  "salesforce": {
      "stage":{
        "source_object":"OpportunityStage",
        "columns":{
          "order":{
            "source_column_expression":"ISNULL(SortOrder, 0)"
          },
          ...
```
### Custom SQL
In even more elaborate cases, you might need to provide your own SQL query. You can do so by providing `extract_sql`.
```
"dataset_mapping": {
  "salesforce": {
    "activity_monitoring":{
      "extract_sql":"activity_monitoring.sql"
    },
```