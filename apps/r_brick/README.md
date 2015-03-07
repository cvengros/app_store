R Brick
=============
Brick for executing R code.

Data from specified ADS tables -> your R script -> data to specified ADS tables.

## Params
- `R_SCRIPT_DIR` (String): Directory where the script is. Can be a local path - relative to the deployed app (e.g. `'r/scripts'`), or s3 (`s3://bucket/path`). In case s3 is used, you have to provide blah blah XXXX `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `R_SCRIPT_FILENAME`
- `ADS_ID`, `ADS_USERNAME`, `ADS_PASSWORD` (hidden): ADS credentials
- `INPUT_TABLES` (Array): Names of tables that are to be fed into the R script. Each given table is exported to a CSV and given to the R script. E.g. if you want to predict sales in R based on the sales table, you'll pass ['sales'] to the brick. The table will be exported to `/tmp/something/sales.csv` which will be passed to your scripts in the `input_filenames` R variable. 
- `OUTPUT_TABLES` (Array): Any output that your R script generates can be saved back to ADS. Save the results to the file that you got in the `output_tables` R variable. The order of filenames corresponds to order you provided in the OUTPUT_TABLES param. The brick will save the output data to ADS. E.g. you generated predictions and want it to be saved to the `predictions` table. You obtain the filename `/tmp/something/predictions.csv` from the `output_tables` variable. You write your results to the file. The brick will import the data to the `predictions` table in ADS.

## Examples
Example R script, that runs a prediction on top of a scenario data and saves the preditions (score) `predict.r`:
```r
suppressPackageStartupMessages(require(randomForest, quietly=TRUE))

# read the scenario - the path comes in as the first member
# of the input_filenames param
scenario_file <- head(input_filenames, n=1)
scenario <- read.csv(scenario_file)

# load the model - path is relative to the directory 
# given in R_SCRIPT_DIR param
load('rfmodel.rmd')

# predict the score 
score <- predict(rf, scenario)

# write it to a file given as the first member of the output_filenames param
output_file <- head(output_filenames, n=1)
write.csv(score, output_file)

```
Brick params when running R script contained in the brick folder, path is relative to the brick folder.
```json
{
  "R_SCRIPT_DIR": "r_scripts/my",
  "R_SCRIPT_FILENAME": "predict.r",
  "ADS_ID": "ads id (just the last part)",
  "ADS_USERNAME": "you@gooddata.com",
  "ADS_PASSWORD": "your pass",
  "INPUT_TABLES": "scenario",
  "OUTPUT_TABLES": "score"
}
```
Brick params when running R script that is on S3.
```json
{
  "R_SCRIPT_DIR": "s3n://my-bucket/ha/r_scripts/my",
  "R_SCRIPT_FILENAME": "predict.r",
  "ADS_ID": "ads id (just the last part)",
  "ADS_USERNAME": "you@gooddata.com",
  "ADS_PASSWORD": "your pass",
  "INPUT_TABLES": "scenario",
  "OUTPUT_TABLES": "score",
  "AWS_ACCESS_KEY_ID": "your id",
  "AWS_SECRET_ACCESS_KEY": "your secret"
}
```