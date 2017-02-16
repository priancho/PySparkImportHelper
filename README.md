# PySparkImportHelper
A helper class that facilitates uploading a library into Spark's distributed cache while keeping its directory structure

# Usage
```
from import_helper import SparkImportHelper

myhelper = SparkImportHelper(spark)   # spark is SparkSession object
myhelper.add_deps("/path/to/my/lib")
```

All .py files in and under "/path/to/my/lib" will be uploaded into Spark's distributed cache WHILE KEEPING ITS DIRECTORY STRUCTURE.
So, all import statements works without any modification when you run it by both Python and Spark.

# History
2017.02.16 - First commit.
