R Brick
=============
Brick for executing R code.

## Params
- `R_SCRIPT_DIR` (String): Directory where the script is. Can be a local path - relative to the deployed app (e.g. `'r/scripts'`), or s3 (`s3://fdfd/fdfd`). In case s3 is used, you have to provide blah blah XXXX
- `R_SCRIPT_FILENAME`
- `ADS_ID`, `ADS_USERNAME`, `ADS_PASSWORD` (hidden): ADS credentials
- `INPUT_TABLES` (Array): Names of tables that are to be fed into the R script. Each given table is exported to a CSV and given to the R script. E.g. if you want to predict sales in R based on the sales table, you'll pass ['sales'] to the brick. The table will be exported to `/tmp/something/sales.csv` which will be passed to your scripts in the `input_filenames` R variable. 
- `OUTPUT_TABLES` (Array): Any output that your R script generates can be saved back to ADS. Save the results to a csv file and pass the file to the `output_tables` R variable. The brick will save the output data to ADS. E.g. you generated predictions and want it to be saved to the `predictions` table. You save the data to the `predictions.csv` file and assign the `output_filenames` R variable to `['predictions.csv']`. The brick will import the data to the `predictions` table in ADS.