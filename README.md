# Referential Salesapp implementation on top of Ruby bricks and DSS

The implementation is highly modular and configurable. It consist of 2 bricks that run independently (`salesapp_sfdc_to_dss` and `salesapp_dss_to_gdc`) Each piece then consist of two smaller bricks that can be run independently i.e. you when you've already downloaded data from salesforce you can only run the part that saves data to DSS. The visual structure is below:
![Overview](docs/overview.png)

## How to make it work:
Clone the repository fork:
`git clone https://github.com/cvengros/app_store`
`cd app_store`

Check out the right branch
`git checkout salesapp`

Choose to run on jruby (if you don't have rvm installed, it's a good time now.)
`rvm use jruby`

Install the dependencies
`bundle install`

There are two bricks you want to use: `salesapp_sfdc_to_dss` and `salesapp_dss_to_gdc`. 

Now you need to configure the bricks. A typical setup for the `salesapp_sfdc_to_dss` is in `apps/salesapp_sfdc_to_dss/typical-params.json`. Create a file `apps/salesapp_sfdc_to_dss/params.json`, copy the contents of the typical params and fill in the passwords and other stuff that's specific for you. The same applies to the `salesapp_dss_to_gdc` brick: typical params are in `apps/salesapp_dss_to_gdc/typical-params.json` should go to `apps/salesapp_dss_to_gdc/params.json`.

Description of all the parameters is in `apps/salesapp_sfdc_to_dss/example-params.json` and `apps/salesapp_dss_to_gdc/example-params.json`

You'll need a Salesforce OAuth application, see [the manual](http://www.salesforce.com/us/developer/docs/api_rest/Content/quickstart_oauth.htm#step1_oauth). For that you need a salesforce account, e.g. a [developer edition account](https://developer.salesforce.com/signup).

To load data to GD you need to have a GoodSales essentials project with date facts removed. One such project is `agt0l6aqqk2wgkqdqnh6qr4u2jhfvd1w`, ask Petr Olmer for invitation.

When you've set up all the params, use the following command to run the `salesapp_sfdc_to_dss` locally:

    bundle exec gooddata -lv -U you@gooddata.com -P yourpassword -s https://secure.gooddata.com -p projectidreldkfjalnio run_ruby -d apps/salesapp_sfdc_to_dss --name "my extract process" --params apps/salesapp_sfdc_to_dss/params.json

`salesapp_dss_to_gdc`:

    bundle exec gooddata -lv -U you@gooddata.com -P yourpassword -s https://secure.gooddata.com -p projectidreldkfjalnio  run_ruby -d apps/salesapp_dss_to_gdc --name "my transform load process" --params apps/salesapp_dss_to_gdc/params.json





