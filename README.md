# Referential GoodSales Essentials implementation

## What is it?
It's a referential implementation of GoodSales Essentials ETL on top of BI automation layer (Ruby bricks) and ADS (DSS). It highlights the best practices for implementing BI projects:
* Store all data in DSS
* Do full loads to GD
* Have a separate process for downloading from source to DSS and another  process for transforming and loading from DSS to GD. These can scheduled independently.
* All processes are restartable.
* Individual pieces of the processes can be run separately for easy debugging.
* The downloader doesn't write directly to DSS, it just downloads CSVs. Data are loaded in a separate brick.
* Raw data are backed up to S3.
* Transformations are done by generated or explicity given SQLs on top of DSS.
* Transformations don't load data directly to GoodData projects. GD upload is done by a separate brick.
* Each dataset has its own transformation which is independent of all others.
* Data that are used for non-analytic purposes (user filters) follow the same process as analytic data. They are loaded to DSS and extracted by SQLs.
* Downloader downloads all available data, no matter which fields are currently used.
* Credentials are separated from the rest of the parameters. 

## Configuration and customization
* Each little piece can be configured.
* There are presets holding the most usual configuration values. You don't have to configure every little piece, if you're just doing standard stuff.
* Each preset piece of configuration can be overriden on it's own, while keeping the rest of the configuration from the preset.
* The implementation is a white box - you can customize whatever piece you need. Most of the changes you need to make are done through configuration. If that's not enough - it's all just Ruby and SQL, so you can change whatever you want.

* Most of the code is in repositories outside of app store. Bricks just care about orchestration and configuration.

## The big picture
![Overview](docs/overview.png)
There two big bricks - `salesforce_dss` and `dss_gd`. These are composed of a number of small bricks. Each brick (small or big) can be run separately.

The ETL is divided to two processes:

1. "E" `salesforce_dss`: Downloads data from Salesforce and loads it to DSS. This process is meant to be run a few times each day. It's composed of the following bricks:
  1. `salesforce_csv`: Downloads data from Salesforce to local CSVs.
  2. `hierarchy`: Recursively unpacks the hierarchy downloaded from Salesforce to pairs user, boss. Only performed if UserRole is in downloaded objects.
  3. `csv_dss`: Creates tables in DSS (if they don't exist yet) and uploads the local CSV there. 
2. "TL" `dss_gd`: Extracts data from Salesforce and loads it to GD. This process is meant to be run once a day. It's composed of the following bricks:
  1. `dss_csv`: Extracts data from DSS to local CSVs using generated and user-defined SQL queries. There's one SQL for each GD dataset, plus one for synchronizing users.
  2. `csv_gd`: Loads datasets from local CSVs to GD.
  3. `user_sync`: Synchronizes users in the domain and in the project according to the local CSV with users.
  4. `user_filters`: Creates data filters (MUFs) for each user so that they only see opportunities they should see.

## How to run a brick
bundle exec kdesi cosi
### Configuration
udelej si configuration dir 
jak se to pretezuje


## Structure of each brick
Most of the functionality is implemented outside of bricks in a separate gem. Bricks just hold configuration presets and orchestrate running code. Each brick has a directory in the app directory
Each brick has the following structure:
* `config` directory contains configuration presets for the brick. 
  * `credentials.json` define which credentials need to be passed to the brick with example values.
  * `gse.json` is a preset for the most common GSE implementation.
* `main.rb` holds the definition of the pipeline - which middlewares and small bricks are used.
* `<brick name>.rb` holds defintion of the brick specific classes. Execute defines how the contained bricks are orchestrated. 







