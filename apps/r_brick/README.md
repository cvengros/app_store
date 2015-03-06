R Brick
=============
Brick for executing R code.

Data from specified ADS tables -> your R script -> data to specified ADS tables.

## Params
- `R_SCRIPT_DIR` (String): Directory where the script is. Can be a local path - relative to the deployed app (e.g. `'r/scripts'`), or s3 (`s3://fdfd/fdfd`). In case s3 is used, you have to provide blah blah XXXX
- `R_SCRIPT_FILENAME`
- `ADS_ID`, `ADS_USERNAME`, `ADS_PASSWORD` (hidden): ADS credentials
- `INPUT_TABLES` (Array): Names of tables that are to be fed into the R script. Each given table is exported to a CSV and given to the R script. E.g. if you want to predict sales in R based on the sales table, you'll pass ['sales'] to the brick. The table will be exported to `/tmp/something/sales.csv` which will be passed to your scripts in the `input_filenames` R variable. 
- `OUTPUT_TABLES` (Array): Any output that your R script generates can be saved back to ADS. Save the results to the file that you got in the `output_tables` R variable. The order of filenames corresponds to order you provided in the OUTPUT_TABLES param. The brick will save the output data to ADS. E.g. you generated predictions and want it to be saved to the `predictions` table. You obtain the filename `/tmp/something/predictions.csv` from the `output_tables` variable. You write your results to the file. The brick will import the data to the `predictions` table in ADS.