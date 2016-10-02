# Alfresco adapter to work with Azure block storage
Initial version developed by the EisenVault development team. Reach us at contact@eisenvault.com
Initial commit on 2nd October 2016

#Credits
Base Code taken from Ryan Berg's repository: https://github.com/rmberg/alfresco-s3-adapter
Modified the above code to work with Azure.


#Disclaimer
We have done basic testing and it seems to work - would appreciate your help in testing it further.
Pull Requests / Issues / Contributions are welcome!
Tested with Alfresco Community 5.0.d and Alfresco Community 5.1.g

#Build Instructions

 * After cloning the project, run `mvn clean install` to download dependencies and build the project

Installation / Configuration

 * After installing the `alfresco-azure-adapter.amp` package you will need to add some properties to your `alfresco-global.properties` file:
 
```
# Your Azure credentials

azure.accessKey=<<your azure storage key>>

# The Azure block content store Container Name
azure.containerName=<<your container name>>

# The Azure block content store Account Name
azure.accountName=<<your account name>>

# The relative path within the bucket to use as the content store
dir.contentstore=contentstore

# The relative path within the bucket to use as the deleted content store
dir.contentstore.deleted=contentstore.deleted
