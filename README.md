
# Rialto

Rialto is a framework for building and deploying machine learning features in a scalable and reusable way. It provides a set of tools that make it easy to define and deploy new features, and it provides a way to orchestrate the execution of these features.

The name Rialto is a reference to the Rialto Bridge in Venice, Italy. The Rialto Bridge was a major marketplace for goods and ideas the Middle Ages.
Rialto is intended to be a foundation of a similar marketplace for machine learning features, where users and find and share reusable features.

Sphinx-Generated autodocs pages available **[here](https://absaoss.github.io/rialto/)**.

# Contents
1. [Installation](#install)
2. [Library Overview](#overview)
3. [Contributing](#contributing)

# <a id="install"></a> 1. Installation
```bash
pip install rialto
```

# <a id="overview"></a> 2. Library Overview
This library currently contains:
1. [runner](#runner)
2. [maker](#maker)
3. [jobs](#jobs)
3. [loader](#loader)
4. [metadata](#metadata)
5. [common](#common)

## <a id="runner"></a> 2.1 - runner

Runner is the orchestrator and scheduler of Rialto. It can be used to execute any [job](#jobs) but is primarily designed to execute feature [maker](#maker) jobs.
The core of runner is execution of a Transformation class that can be extended for any purpose and the execution configuration that defines the handling of i/o, scheduling, dependencies and reporting.

### Transformation
For the details on the interface see the [implementation](rialto/runner/transformation.py)
Inside the transformation you have access to a [TableReader](#common), date of running, and if provided to Runner, a live spark session and [metadata manager](#metadata).
You can either implement your jobs directly via extending the Transformation class, or by using the [jobs](#jobs) abstraction.

### Runner

Bellow is the minimal code necessary to execute the runner.

```python
from rialto.runner import Runner
# Create the runner instance
runner = Runner(spark, config_path=cfg)
# Execute the run
runner()
```

A runner by default executes all the jobs provided in the configuration file, for all the viable execution dates according to the configuration file for which the job has not yet run successfully (i.e. the date partition doesn't exist on the storage)
This behavior can be modified by various parameters and switches available.

* **run_date** - date at which the runner is triggered (defaults to day of running)
* **rerun** - rerun all jobs even if they already succeeded in the past runs
* **op** - run only selected operation / pipeline
* **skip_dependencies** - ignore dependency checks and run all jobs
* **overrides** - dictionary of overrides for the configuration


Transformations are not included in the runner itself, it imports them dynamically according to the configuration, therefore it's necessary to have them locally installed.

### Configuration

```yaml
runner:
  watched_period_units: "months" # unit of default run period
  watched_period_value: 2 # value of default run period
  mail: # optional email reporting configuration
    to: # a list of email addresses
      - name@host.domain
      - name2@host.domain
    sender: rialto.noreply@domain # what a sender should say
    smtp: smtp.server.url # your smtp server
    subject: "Rialto report" # Email subject header
  bookkeeping: catalog.schema.table # optional table where bookkeeping information is stored

pipelines: # a list of pipelines to run
- name: Pipeline1  #Pipeline name & a name of table it creates (table will be converted to have _ instead of uppercase letters)
  module: # Python transformation class location
    python_module: module_name
    python_class: Pipeline1Class
  schedule:
    frequency: weekly # daily/weekly/monthly
    day: 7 # day of the week or month
    info_date_shift: #Optional shift in the written information date from the scheduled day
      - units: "days" # days/weeks/months/years
        value: 5 # subtracted from scheduled day
  dependencies: # list of dependent tables
    - table: catalog.schema.table1
      name: "table1" # Optional table name, used to recall dependency details in transformation
      date_col: generation_date # Mandatory date column name
      interval: # mandatory availability interval, subtracted from scheduled day
        units: "days"
        value: 1
    - table: catalog.schema.table2
      name: "table2"
      interval:
        units: "months"
        value: 1
  target:
      target_schema: catalog.schema # schema where tables will be created, must exist
      target_partition_column: INFORMATION_DATE # date to partition new tables on
  metadata_manager: # optional
      metadata_schema: catalog.metadata # schema where metadata is stored
  feature_loader: # optional
      feature_schema: catalog.feature_tables # schema where feature tables are stored
      metadata_schema: catalog.metadata # schema where metadata is stored
  extras: #optional arguments processed as dictionary
    some_value: 3
    some_other_value: giraffe

- name: PipelineTable1 # will be written as pipeline_table1
  module:
    python_module: module_name
    python_class: Pipeline2Class
  schedule:
    frequency: monthly
    day: 6
    info_date_shift: # can be combined as a list
      - units: "days"
        value: 5
      - units: "months"
        value: 3
  dependencies:
    - table: catalog.schema.table3
      interval:
        units: "days"
        value: 6
  target:
      target_schema: catalog.schema # schema where tables will be created, must exist
      target_partition_column: INFORMATION_DATE # date to partition new tables on
```

The configuration can be dynamically overridden by providing a dictionary of overrides to the runner. All overrides must adhere to configurations schema, with pipeline.extras section available for custom schema.
Here are few examples of overrides:

#### Simple override of a single value
Specify the path to the value in the configuration file as a dot-separated string

```python
Runner(
        spark,
        config_path="tests/overrider.yaml",
        run_date="2023-03-31",
        overrides={"runner.watch_period_value": 4},
    )
```

#### Override list element
You can refer to list elements by their index (starting with 0)
```python
overrides={"runner.mail.to[1]": "a@b.c"}
```

#### Append to list
You can append to list by using index -1
```python
overrides={"runner.mail.to[-1]": "test@test.com"}
```

#### Lookup by attribute value in a list
You can use the following syntax to find a specific element in a list by its attribute value
```python
overrides={"pipelines[name=SimpleGroup].target.target_schema": "new_schema"},
```

#### Injecting/Replacing whole sections
You can directly replace a bigger section of the configuration by providing a dictionary
When the whole section doesn't exist, it will be added to the configuration, however it needs to be added as a whole.
i.e. if the yaml file doesn't specify feature_loader, you can't just add a feature_loader.feature_schema, you need to add the whole section.
```python
overrides={"pipelines[name=SimpleGroup].feature_loader":
                           {"feature_schema": "catalog.features",
                            "metadata_schema": "catalog.metadata"}}
```

#### Multiple overrides
You can provide multiple overrides at once, the order of execution is not guaranteed
```python
overrides={"runner.watched_period_value": 4,
           "runner.watched_period_units": "weeks",
           "pipelines[name=SimpleGroup].target.target_schema": "new_schema",
           "pipelines[name=SimpleGroup].feature_loader":
                           {"feature_schema": "catalog.features",
                            "metadata_schema": "catalog.metadata"}
           }
```


## <a id="maker"></a> 2.2 - maker
The purpose of (feature) maker is to simplify feature creation, allow for consistent feature implementation that is standardized and easy to test.

Maker provides the utilities to define feature generating functions by wrapping python functions with provided decorators and a framework to execute these functions.

### FeatureMaker
FeatureMaker is pre-initialized class that provides the means to execute pre-defined feature functions. It has 2 modes, sequential features and aggregated features.
Features need to be defined in a separate importable module as in the examples.

Since there is always only one FeatureMaker object, you can use its state variables from inside feature functions. Specifically
**FeatureMaker.make_date** and **FeatureMaker.key**. These variables are set when calling a FeatureMaker.make(...) function to the values of its parameters.

#### Sequential
Sequential feature generation can be simply interpreted as appending a new column for every feature to the existing dataframe.

```python
from rialto.maker import FeatureMaker
from my_features import simple_features

features, metadata = FeatureMaker.make(
    df=input_data,
    key="KEY",
    make_date=run_date,
    features_module=simple_features,
    keep_preexisting=True)
```

#### Aggregated
In aggregated generation, the source dataframe is grouped by the key or keys and the features themselves are the aggregations put into the spark agg function.

```python
from rialto.maker import FeatureMaker
from my_features import agg_features

features, metadata = FeatureMaker.make_aggregated(
    df=input_data,
    key="KEY",
    make_date=run_date,
    features_module=agg_features)
```

There are also **make_single_feature** and **make_single_agg_feature** available, intended to be used in tests. (See full documentation)

### Features
Features, whether sequential or aggregated ar defined as simple python functions that return a PySpark **function**, that generates the desired column.
In the case of sequential, this function is inserted into PySpark withColumn function as following

```python
data_frame.withColumn("feature_name", RETURNED_FUNCTION_GOES_HERE)
```

in the case of aggregated features, they are used as follows

```python
data_frame.groupBy("key").agg(RETURNED_FUNCTION_GOES_HERE.alias("feature_name"))
```

All the features in one python module are processed in one call of FeatureMakers make functions, therefore you can't mix aggregated and sequential features into one module.

### Decorators
To define Rialto features, the framework provides a set of decorators.

#### @feature
This registers the function as a feature. Every feature has to be decorated with @feature as the **outermost** wrapper.

```python
import pyspark.sql.functions as F
import rialto.maker as rfm
from pyspark.sql import Column
from rialto.metadata import ValueType as VT

@rfm.feature(VT.numerical)
def RECENCY() -> Column:
    return F.months_between(F.lit(rfm.FeatureMaker.make_date), F.col("DATE"))
```

@feature takes one parameter, [metadata](#metadata) enum of value type.

#### @desc
Provides an option to pass a string describing the feature that is then saved as part of feature [metadata](#metadata).

```python
import pyspark.sql.functions as F
import rialto.maker as rfm
from pyspark.sql import Column
from rialto.metadata import ValueType as VT

@rfm.feature(VT.numerical)
@rfm.desc("Recency of the action")
def RECENCY() -> Column:
    return F.months_between(F.lit(rfm.FeatureMaker.make_date), F.col("DATE"))
```

#### @param
Inspired by @pytest.mark.parametrize, it has similar interface and fulfills the same role. It allows you to invoke the feature function multiple times with different values of the parameter.
If multiple @params are used, the number of final features will be a product of all parameters. The feature function has to expect a parameter with the same name as the @params name.

```python
import pyspark.sql.functions as F
import rialto.maker as fml
from pyspark.sql import Column
from rialto.metadata import ValueType as VT

@fml.feature(VT.numerical)
@fml.param("product", ["A", "B", "C"])
@fml.param("status", ["ACTIVE", "INACTIVE"])
@fml.desc("Number of given products with given status")
def NUM(product: str, status: str) -> Column:
    return F.count(F.when((F.col("PRODUCT") == product) & (F.col("STATUS") == status), F.lit(1)))
```

The above code creates following features
* NUM_PRODUCT_A_STATUS_ACTIVE
* NUM_PRODUCT_A_STATUS_INACTIVE
* NUM_PRODUCT_B_STATUS_ACTIVE
* NUM_PRODUCT_B_STATUS_INACTIVE
* NUM_PRODUCT_C_STATUS_ACTIVE
* NUM_PRODUCT_C_STATUS_INACTIVE


#### @depends
@depends allows for a definition of features that depend on each other / need to be calculated in certain order.
**Dependency resolution uses the raw feature names (function names)** not parameter expanded names.

```python
import pyspark.sql.functions as F
import rialto.maker as rfm
from pyspark.sql import Column
from rialto.metadata import ValueType as VT

@rfm.feature(VT.numerical)
@rfm.desc("Recency of the action")
def RECENCY() -> Column:
    return F.months_between(F.lit(rfm.FeatureMaker.make_date), F.col("DATE"))


@rfm.feature(VT.numerical)
@rfm.desc("Action month delay")
@rfm.param("month", [1, 2, 3])
@rfm.depends("RECENCY")
def DELAY(month) -> Column:
    recency = F.col("RECENCY")
    return F.when((recency < month) & (recency >= month - 1), F.lit(1))
```

In this example, @depends, ensures that RECENCY is calculated before DELAY

### Feature name
Publicly available as rialto.maker.utils.feature_name(...), this function is used to create final names of all features.
It concatenates the name of the python function with any given @params and their values used in that instance of the feature.


## <a id="jobs"></a> 2.3 - jobs
Rialto jobs simplify creation of runner transformations. Instead of having to inherit the base *Transformation* class, this module offers two decorators: *datasource*, and *job*.

As the names might suggest,
* *datasource* registers the function below as a valid datasource, which can be used as dependency
* *job* registers the decorated function as a Rialto transformation.

The output / return value of both functions **should/has to be** a python dataframe *(or nothing for jobs, more on that later)*.

### rialto job dependencies
Both jobs and datasources can request dependencies as function arguments. Rialto will attempt to resolve those dependencies and provide them during the job run(s).

We have a set of pre-defined dependencies:
* **run_date** gives you the *date* on which the job is supposed to run
* **spark** contains the *spark session*
* **config** returns the custom config you can specify with each job run
* **dependencies** returns a dictionary containing the job dependencies config
* **job_metadata** returns *JobMetadata* containing information about the job package
* **table_reader** returns *TableReader*
* **feature_loader** provides *PysparkFeatureLoader*
* **metadata_manager** provides *MetadataManager*

Apart from that, each **datasource** also becomes a fully usable dependency. Note, that this means that datasources can also be dependent on other datasources - just beware of any circular dependencies!

With that sorted out, we can now provide a quick example of the *rialto.jobs* module's capabilities:

```python
from pyspark.sql import DataFrame
from rialto.common import TableReader
from rialto.jobs import config_parser, job, datasource
from rialto.runner.config_loader import PipelineConfig
from pydantic import BaseModel


class ConfigModel(BaseModel):
    some_value: int
    some_other_value: str


@config_parser
def my_config(config: PipelineConfig):
    return ConfigModel(**config.extras)


@datasource
def my_datasource(run_date: datetime.date, table_reader: TableReader) -> DataFrame:
    return table_reader.get_latest("my_catalog.my_schema.my_table", date_until=run_date)


@job
def my_job(my_datasource: DataFrame, my_config: ConfigModel) -> DataFrame:
    return my_datasource.withColumn("HelloWorld", F.lit(my_config.some_value))
```
This piece of code
1. creates a rialto transformation called *my_job*, which is then callable by the rialto runner.
2. It sources the *my_datasource* and then runs *my_job* on top of that datasource.
3. Rialto adds VERSION (of your package) and INFORMATION_DATE (as per config) columns automatically.
4. The rialto runner stores the final to a catalog, to a table according to the job's name.

### Custom job names
Note, that by default, the rialto job name is your function name. To allow more flexibility, we allow renaming of the job:
```python
@job(custom_name="my_custom_name")
def f(...):
    ...
```
Just note that any *WeirdCaseNames* will be transformed to *lower_case_with_underscores*.

### Disabling Versioning
If you want to disable versioning of your job (adding package VERSION column to your output):

```python3
@job(disable_version=True)
def my_job(...):
    ...
```
These parameters can be used separately, or combined.

### Notes & Rules
The rules for the dependencies are fairly straightforward.
Both **jobs** and **datasources** can only depend on *pre-defined* dependencies and other *datasources*. Meaning:
* *datasource -> datasource -> job* is perfectly fine,
* *datasource -> job -> datasource* will result in an error.

Secondly, the jobs can, but **don't necessarily have to output** a dataframe.
In case your job doesn't output a dataframe, your job will return an artificially-created, one-row dataframe, which will ensure that rialto notices that the job ran successfully.
This can be useful in **model training**.

Finally, remember, that your jobs are still just *Rialto Transformations* internally.
Meaning that at the end of the day, you should always read some data, do some operations on it and either return a pyspark DataFrame, or not return anything and let the framework return the placeholder one.


### Importing / Registering Datasources
Datasources required for a job (or another datasource) can be defined in a different module.
To register your module as a datasource, you can use the following functions:

```python3
from rialto.jobs import register_dependency_callable, register_dependency_module
import my_package.my_datasources as md
import my_package.my_datasources_big as big_md

# Register an entire dependency module
register_dependency_module(md)

# Register a single datasource from a bigger module
register_dependency_callable(big_md.sample_datasource)

@job
def my_job(my_datasource, sample_datasource: DataFrame, ...):
    ...
```

Each job/datasource can only resolve datasources it has defined as dependencies.

**NOTE**: While ```register_dependency_module``` only registers a module as available dependencies, the ```register_dependency_callable``` actually brings the datasource into the targed module - and thus becomes available for export in the dependency chains.


### Testing
One of the main advantages of the jobs module is simplification of unit tests for your transformations. Rialto provides following tools:

#### 1. Disabling Decorators

Assuming we have a my_package.test_job_module.py module:
```python3
@datasource
def datasource_a(...)
    ... code...

@job
def my_job(datasource_a, ...)
    ... code...
```
The *disable_job_decorators* context manager, as the name suggests, disables all decorator functionality and lets you access your functions as raw functions - making it super simple to unit-test:

```python3
from rialto.jobs.test_utils import disable_job_decorators
import my_package.test_job_module as tjm


# Datasource Testing
def test_datasource_a():
    ... mocks here...

    with disable_job_decorators(tjm):
        datasource_a_output = tjm.datasource_a(...mocks...)

        ...asserts...


# Job Testing
def test_my_job():
    datasource_a_mock = ...
    ...other mocks...

    with disable_job_decorators(tjm):
        job_output = tjm.my_job(datasource_a_mock, ...mocks...)

        ...asserts...
```

#### 2. Testing the @job Dependency Tree
In complex use cases, it may happen that the dependencies of a job become quite complex. Or you simply want to be sure that you didn't accidentally misspelled your dependency name:

```python3
from rialto.jobs.test_utils import resolver_resolves
import my_job.test_job_module as tjm


def test_my_job_resolves(spark):
    assert resolver_resolves(spark, tjm.my_job)
```

The code above fails if *my_job* depends on an undefined datasource (even indirectly), and detects cases where there's a circular dependency.

## <a id="loader"></a> 2.4 - loader
This module is used to load features from feature store into your models and scripts. Loader provides options to load singular features, whole feature groups, as well as a selection of features from multiple groups defined in a config file, and served as a singular dataframe. It also provides interface to access feature metadata.

Two public classes are exposed form this module. **DatabricksLoader**(DataLoader), **PysparkFeatureLoader**(FeatureLoaderInterface).

### PysparkFeatureLoader

This class needs to be instantiated with an active spark session, data loader and a path to the metadata schema (in the format of "catalog_name.schema_name").

```python
from rialto.loader import PysparkFeatureLoader

feature_loader = PysparkFeatureLoader(spark= spark_instance, feature_schema="catalog.schema", metadata_schema= "catalog.schema2", date_column="information_date")
```

#### Single feature

```python
from rialto.loader import PysparkFeatureLoader
from datetime import datetime

feature_loader = PysparkFeatureLoader(spark, "feature_catalog.feature_schema", "metadata_catalog.metadata_schema")
my_date = datetime.strptime("2020-01-01", "%Y-%m-%d").date()

feature = feature_loader.get_feature(group_name="CustomerFeatures", feature_name="AGE", information_date=my_date)
metadata = feature_loader.get_feature_metadata(group_name="CustomerFeatures", feature_name="AGE")
```

#### Feature group
This method of data access is only recommended for experimentation, as the group schema can evolve over time.

```python
from rialto.loader import PysparkFeatureLoader
from datetime import datetime

feature_loader = PysparkFeatureLoader(spark, "feature_catalog.feature_schema", "metadata_catalog.metadata_schema")
my_date = datetime.strptime("2020-01-01", "%Y-%m-%d").date()

features = feature_loader.get_group(group_name="CustomerFeatures", information_date=my_date)
metadata = feature_loader.get_group_metadata(group_name="CustomerFeatures")
```

#### Configuration

```python
from rialto.loader import PysparkFeatureLoader
from datetime import datetime

feature_loader = PysparkFeatureLoader(spark, "feature_catalog.feature_schema", "metadata_catalog.metadata_schema")
my_date = datetime.strptime("2020-01-01", "%Y-%m-%d").date()

features = feature_loader.get_features_from_cfg(path="local/configuration/file.yaml", information_date=my_date)
metadata = feature_loader.get_metadata_from_cfg(path="local/configuration/file.yaml")
```

The configuration file is expected to be in a yaml format and has 3 sections: selection, base, maps.
* **selection** is a list of feature groups and desired features in those groups. Each group also needs a prefix defined, which will prefix a name of every feature from that group
* **base** is a feature group with a set of keys defined, a unique selection of these keys from this group will form the base of the resulting dataframe and all feature will be left-joined onto that
* **maps** is an optional list of dataframes that will be joined onto the base as a whole and their purpose is to bridge feature sets with different primary keys, i.e. we have a map linking IDnumbers and AccountNumbers so we can use feature groups based on either of the primary keys.


```yaml
selection:
  - group: Group_A
    prefix: A_PREFIX
    features:
      - Feature_A1
      - Feature_A2
  - group: Group_B
    prefix: B_PREFIX
    features:
      - Feature_B1
      - Feature_B2
base:
  group: Group_D
  keys:
    - Column_Key1
    - Column_Key2
maps:
  - MapGroup1
  - MapGroup2
```



## <a id="metadata"></a> 2.5 - metadata
Rialto metadata module is designed specifically to work with features. It's a support function to pass data from [maker](#maker) to [loader](#loader), and to have additional metadata available for further features processing.

Metadata module consists of 3 parts: enums, metadata dataclasses and MetadataManager.

### enums
There's two enums available used across Rialto, **Schedule** and **ValueType**. Schedule defines the frequency of execution (daily, weekly, monthly, yearly) and ValueType defines the feature type (nominal, ordinal, numeric)

### data classes

There are 2 metadata data classes: FeatureMetadata and GroupMetadata. They are used as containers to pass metadata information around, and are the return types of metadata requests in loader.

```python
FeatureMetadata
    value_type: ValueType # type of feature value
    name: str # feature name
    description: str # feature description
    group: GroupMetadata # feature group this feature belongs to
```

```python
GroupMetadata
    name: str # group name (Original name mirroring the transformation class name)
    frequency: Schedule # generation frequency
    description: str # group description
    key: List[str] # group primary keys
    owner: str # owner of the group data (table)
    fs_name: str = None # actual table name of this feature group in DataBricks
    features: List[str] = None # A list of feature names belonging to this group
```

### MetadataManager
This class manages the metadata and provides the interface to fetch or write. Metadata is stored in 2 dataframes **group_metadata** and **feature_metadata**, in the schema provided as a parameter.



## <a id="common"></a> 2.6 - common
Common houses functions and utilities used throughout the whole framework.
Most importantly it defines _DataReader_ class which is implemented as _TableReader_ with two public functions, _get_latest(...)_ and _get_table(...)_.

_TableReader_ can be used as a standalone delta or parquet table loader.

initialization:
```python
from rialto.common import TableReader

reader = TableReader(spark=spark_instance)
```

usage of _get_table_:

```python
# get whole table
df = reader.get_table(table="catalog.schema.table", date_column="information_date")

# get a slice of the table
from datetime import datetime

start = datetime.strptime("2020-01-01", "%Y-%m-%d").date()
end = datetime.strptime("2024-01-01", "%Y-%m-%d").date()

df = reader.get_table(table="catalog.schema.table", date_from=start, date_to=end, date_column="information_date")
```

usage of _get_latest_:

```python
# most recent partition
df = reader.get_latest(table="catalog.schema.table", date_column="information_date")

# most recent partition until
until = datetime.strptime("2020-01-01", "%Y-%m-%d").date()

df = reader.get_latest(table="catalog.schema.table", date_until=until, date_column="information_date")

```
For full information on parameters and their optionality see technical documentation.

_TableReader_ needs an active spark session and an information which column is the **date column**.
There are three options how to pass that information on.


# <a id="contributing"></a> 3. Contributing
Contributing:
* Implement the change in a custom branch split off **develop**
* Create a pull request from your **branch** to **develop**
* Get PR approved, merged to **develop**

Releasing:
* Create a Release/1.x.y branch off **develop**
* Bump all necessary versions
* Create PR from release branch to **MASTER**
* Merge to **master** with a **merge commit**
* Actions will fast-forward new commits from master to develop and deploy new version to pypi repository
