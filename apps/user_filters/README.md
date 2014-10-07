# user_filters
Setting up user filters. From a CSV with a list of users and it sets up user filters. Permissions are based on a permission table that is loaded separately. The created user filter mean "User can see the object if there's a connection between the two in the permission table."

## Features
* Creating permission based user filters for all given users.

## Configuration
* `user_column` the name of the CSV column from which the user logins are taken
* `label` is obj id of the connection point label of the user dataset. Example: `gooddata_user` is the user dataset. It has an attribute `attr.gooddata_user.id`. The connection point is its label `label.gooddata_user.id`. The label has obj id `45019`.
* `column` is the CSV column from where the user dataset connection point is coming. E.g. The user permission dataset has a connection point `id` that contains Salesforce Ids. The values are taken from the id column.
* `over` is the user permssion dataset connection point. E.g. the permission dataset is called `user_permission`. It has a connection point `attr.user_permission.factsof`. This conneciton point has obj id `45017`
* `to` is the connection point of the object table. E.g. I want to restrict users from seeing all opportunities. Opportunity dataset has a connection point `attr.opportunity.id` which has an obj id `978`.

## Example
```
{
  "setup": {
    "user_column": "email",
    "labels": [
      { "label": 45019, "column": "id", "over": 45017, "to": 978 }
    ]
  }
}
```
This setup will create a filter for each user in the given CSV with expression like: 
```
([/gdc/md/agt0l6aqqk2wgkqdqnh6qr4u2jhfvd1w/obj/45018] IN 
  ([/gdc/md/agt0l6aqqk2wgkqdqnh6qr4u2jhfvd1w/obj/45018/elements?id=27])) 
  OVER [/gdc/md/agt0l6aqqk2wgkqdqnh6qr4u2jhfvd1w/obj/45017] 
  TO [/gdc/md/agt0l6aqqk2wgkqdqnh6qr4u2jhfvd1w/obj/978])"
```
This can be traslated as:
```
<user_id> IN <the value of the id from the csv> OVER <permissions dataset> TO <opportunity_id>
```