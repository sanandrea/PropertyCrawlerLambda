# Property Crawler
This project contains the business logic of the Lambda functions that Property Crawler has.

The crawler does a search in a predefined area of Ireland. The search result is then stored in an AWS DDB table.
The table has enabled versioning so price changes are recorded with historical values.

## High Level

This package contains only the source python code for the lambda functions.
The code is deployed with cdk from the *sister package* [PropertyCrawlerCDK](https://github.com/sanandrea/PropertyCrawlerCDK)


### Build

To build the python package run the following:

```
source .venv/bin/activate
python setup.py ldist
```

The dependencies of the projects need to declared in the setup.py file in order to be included in the lambda zip.


### Scripts
To run scripts on the items:
1. Create an executable file on the scripts folder (`test.py`).
2. Modify the `setup.py` to include the script in the relative attribute
3. Install the scripts: `python setup.py install`
4. Run the script: *`test.py`*


### Unit tests

Some unit tests depend on dynamodb local. You can download and unzip the local dynamodb from [aws docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html)

After the zip in unpacked run the following Java command in the folder:

```
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb
```


To run unit tests
```
python setup.py install
pytest
```



### TODO 
2. Improve filtering of properties
3. Add GSI for better filtering

