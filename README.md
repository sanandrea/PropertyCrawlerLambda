# Property Crawler
This project contains the business logic of the Lambda functions that Property Crawler has.

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

### TODO 
1. Add alarms in CDK
2. Improve filtering of properties
3. Add GSI for better filtering

