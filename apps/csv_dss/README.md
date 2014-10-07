# csv_dss
Loads data from local CSVs to an ADS instance. If the appropriate tables don't exist yet, they are created.

## Features
* Supports historization, changes are saved as events, per record.
* Can reconstruct history from event-based data (E.g. OpportunityHistory).
* Merging of a new version is done through vertica analytic functions, versions are compared through a hash of all columns. 
* For historized entities, views for last snapshot are created automatically.
* Each load is associated with a load id, across all tables. For each load metadata can be saved. 

## Data model
Data in ADS are stored in a data model described below. Each table has a surrogate key in a column called `_oid`. Names of the tables can have two prefixes - a prefix given in params (e.g. a customer name), and a data source prefix, e.g. salesforce.

Column types are determined from metadata passed in params. 

Each entity can be (but doesn't have to be) historized. For non-historized entities, only the last version is stored. There's one table for each non-historized entity. For historized entities there's a staging table for loading the last version, e.g. `dev_salesforce_Opportunity_in` and a table for keeping the events, e.g. `dev_salesforce_Opportunity`

### Load table
Info about loads is stored in a load table. Each load is represented by a row in this table. 
* `_LOAD_ID` is a unique id of a load, load ids are ordered, so that later load gets a bigger load id.
* `_LOAD_AT` is a date when the load happened. This is passed on the application (Ruby) level.
* `_INSERTED_AT` is a date when the row was physically added to the table. This is taken on a databse level.
* `salesforce_server` is a metadata field, indicates which salesforce server the data was loaded from. 

Example `dev_salesforce_meta_loads`: 

| `_oid` | `salesforce_server` | `_LOAD_ID` | `_LOAD_AT` | 
|------|-------------------|----------|----------|
| 500001 | https://na12.salesforce.com/  |  1411083782 | 2014-09-23 00:58:19 |
| 1 |  https://na12.salesforce.com/ |  1411083882 | 2014-09-19 01:44:42 |
| 250001 | https://na12.salesforce.com/  | 1411418180 | 2014-09-22 22:36:20 |
| 750001 | https://na12.salesforce.com/ | 1411581012 | 2014-09-24 19:50:12 |

### Non-historized entities
Non-historized entities have tables named with the `_in` suffix.
Each entity field has a column in the table. In addition to that, there are metadata columns `_oid`, `_DIFF_HASH`,`_LOAD_ID`,`_LOAD_AT`,`_INSERTED_AT`,`_IS_DELETED`.

Example `dev_salesforce_Opportunity_in`: 

| `_oid` | `Id` | `Name` | `StageName` | `_DIFF_HASH` |`_LOAD_ID` |`_LOAD_AT` |`_INSERTED_AT` |`_IS_DELETED` |
|--------|------|--------|-------------|--------------|----------|------------|---------------|--------------|
| 750001 | 006U0000004I67mIAC | Edge SLA| Closed Won | 55236772193117068528158595386828605880 | 1411581012 | 2014-09-24 19:50:12 | 2014-09-24 19:50:26 | false |
| 750004 | 006U0000004I67hIAC | Express Logistics Portable Truck Generators| Needs Analysis | 4071389736995126037686511083595415808 | 1411581012 | 2014-09-24 19:50:12 | 2014-09-24 19:50:26 | false |
| 750012 | 006U0000004I67kIAC | United Oil Installations| Negotiation/Review | 9581798453570058489011890507887415115 | 1411581012 | 2014-09-24 19:50:12 | 2014-09-24 19:50:26 | false |
| 750016 | 006U0000004I67uIAC | United Oil Installations| Closed Won | 83601151995549062162460313936026408143 | 1411581012 | 2014-09-24 19:50:12 | 2014-09-24 19:50:26 | false |
| 750018 | 006U0000004I67pIAC | United Oil Refinery Generators| Perception Analysis | 48476887272859249994824971350821603893 | 1411581012 | 2014-09-24 19:50:12 | 2014-09-24 19:50:26 | false |
The structure is the same as in staging tables for historized entities. 

### Historized entities
Versions are saved as events for historized entities. Whenever the entity changes a new line appears in the table. If there's no change to the entity, no extra data is stored - which saves a lot of space. 
Additional columns that are stored for historized entities:
* `_VALID_FROM`: Date when the change was recognized. When history is constructed from a history entity this is the time of change given by the source system. For subsequent loads, this is the load date when the change was recognized. 
* `_IS_DELETED`: indicates whether the record was deleted.
* `_LAST_SEEN_ID`: id of the load where the version was last seen in the source system.
Example `dev_salesforce_Opportunity`:

| `_oid` | `Id` | `Name` | `StageName` | `_DIFF_HASH` |`_LOAD_ID` |`_LOAD_AT` |`_INSERTED_AT` |`_IS_DELETED` | `_VALID_FROM` | `_LAST_SEEN_ID` |
|--------|------|--------|-------------|--------------|----------|------------|---------------|--------------|---------------|-----------------|
|750018| 006U0000004I67aIAC | Grand Hotels Kitchen Generator | Id. Decision Makers | 5483555782770426935495573719536921776 | 1411083882 | 2014-09-19 01:44:42 | 2014-09-19 01:45:17 | false | 2011-11-23 22:55:24  | 1411083882 |
|750018| 006U0000004I67aIAC | Grand Hotels Kitchen Generator | Negotiation/Review | 5483555782770426935495573719536921776 | 1411084082 | 2014-09-25 01:44:42 | 2014-09-25 01:45:17 | false | 2014-09-25 01:44:42  | 1411084082 |
|750018| 006U0000004I67aIAC | Grand Hotels Kitchen Generator | Closed Won | 5483555782770426935495573719536921776 | 1411084982 | 2014-10-19 01:44:42 | 2014-10-19 01:45:17 | false | 2014-10-19 01:44:42  | 1411084982 |

### Views
For your convenience, each entity (both historized and non-historized) has a last snapshot view which shows the last known version. View names have the same prefixes as tables, e.g. `dev_salesforce_Opportunity_last_snapshot`.  The schema is the same as described in the Non-historized entities paragraph.
