# user_sync
Seamless user synchronization.

Managing user roles in projects is a complicated problem. This ruby brick allows you to automatically 
ensure your project's users stay up to date. Using a CSV document or a specific set of columns as a single point-of-truth 
the brick will update and change the project to match the document.



## Key Features:
- Add/remove users to project and domain.
- Update user contact information.
- Ignoring certain logins from synchronization (e.g. GoodData people implementing the project), based on regular expression.
- Move users easily between projects
- No programming is required to maintain this brick.
- Update user roles in a project.

This brick in particular was built to allow you to deploy once and never have to think about it again. 

## What you need to get started:
- The domain of the project
- The project ID
- A CSV file containing the user names, first and last name, password, role, and optionally email. 

For example, you have a CSV that looks like this:

    email,login,first_name,last_name,role,password,
    tomas@gooddata.com,tomas777,tomas,svarovsky,Adminstrator,password123,
    patrick@gooddata.com,patrick1,patrick,mcconlogue,Viewer,notapassword1,
    mark@gooddata.com,mark_hamil,mark,hamil,Viewer,anotherpassword
    
The brick's runtime parameters take the path to this file, the project ID, and the domain: 
   
    { 
        :domain => "DOMAIN_OF_PROJECT",
        :project => "PROJECT_ID",
        :csv_path => "./file/path/to/csv"
    }

As a default, the brick will attempt to map data through the columns mentioned in the example. Optionally, you can 
point the brick to different columns for the relevant user information. For instance, if your user roles 
are stored in a column called "privileges", you would pass the params file: 

    {
        :role_column => "privileges"
    }
    
All of the available column options can easily be set with the [Brick Browser](https://secure.getgooddata.com/labs/apps/brick_browser/index.html) or open the info.json in the root directory of the brick.
     
The the User Brick is live and available at the 

## Runtime Notes
- If no email is provided, the login is used in used in it's place.
- Roles are resolved by their identifier ("adminRole"), you can view the complete list [User Roles](https://support.gooddata.com/entries/23297728-User-Roles-Overview)
- Users not found in the CSV will be removed.





