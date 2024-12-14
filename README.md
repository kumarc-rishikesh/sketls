# INTRODUCTION
## scetls - Scala ETLs 
scetls is targeted towards eople working closely with data.
Data transformations and configs are stored in a YAML format enabling readability and version control. 
We aim to solve the problem of messy ETL repos and configs.

## Support
scetls is still in development, and we currently support local instances of PG, Clickhouse and AWS S3([localstack](https://github.com/localstack/localstack))

# USER GUIDE
## Writing the Pipeline YAML
A Data Pipeline config is broken into a Pipeline Config YAML and the transformation definition YAMLs.
Following are the components of the Pipeline YAMLs:
### Pipeline Name and Jobname
Names of the respective Pipeline and Jobs in the pipeline. Jobs are currently set to timeout in 5 minutes. This will be changed in future versions where you would be able to configure timeouts. 
### Source
This is where you would be reading data from. The resource must be running and a client must be obtainable to your instance of the datastore. A tablename and query must be provided in the case of Clickhouse and Postgres. If your source is S3, you must provide an existing bucket and filename in your config.
A trigger must be mentioned and currently cron jobs are supported. The value must be in the crontab format. 
### Transformation
A localtion to the YAML where the Transformation definitions are configured
### Quality Checks
Quality checks can be one of:
- null_check : Checks for nulls in the given column "col"
- type_check : Checks whether the given column in "col" is of the type mentioned in "expected_type"
- range_check : Must only be applied to integer columns. Ensures that the values in the column fall in the range of values given in "min" and "max"
- uniqueness_check : Ensures the values in the column given in "col" are unique
### Destination
This field is similar to the Source fields and describes the configuration of the destination of the data

## Writing the Transformation YAML
These are  individual files for every job. The filepath must be mentioned in the "transformation" field of the Pipeline config
### Name
Name of the Transformation YAML
### Imports
This is a Scala files that define functions to be used in data transformations. Rules for this imported file are defined [here](#scala-transformation-functions)
### Inputs
Columns in the input data and their datatype. As of now, the datatype must be Int or String. Columns mentioned in the inputs but not in the data will throw an error as of now.
In future versions, they will be filled with nulls. Columns in the data but not in the input config will be dropped.
### Outputs
By default, all columns in the input data will be outputted. A function to drop the column is defined. You have to define a column name in "col" and mention "drop" in the "function" with the "type" being input. 
For transformations on input columns, mention the column name in "col", "type" input and the function to be used for the transformation in "function"
Derived columns must have a "type" derived and mention the function that derives the column along with the input columns used mentioned in "using" and the datatype of the output column in "datatype"
## Scala Transformation Functions
These are functions that define transformation must be dealt with great caution. Currently, they provide no type safety or guarantees. They **must** adhere to the following type signatures:
- input type:\
  `(df: DataFrame, ipFieldInfo: String): DataFrame`\
where
`df`: the input dataframe
`ipFieldInfo`: Name of the column to be used
- derived type:\
`(df: DataFrame, ipFieldInfo: Seq[String], opFieldInfo: (String, DataType)): DataFrame`\
where\
`df`: The input dataframe\
`ipFieldInfo`: Fields being used as inputs to this function. The fields **must** exist in the input data\
`opFieldInfo`: A Tuple of the output column's name and the datatype of the output column.

## Branching Guide
New features go int `feat/` beanches derived from the `feature` branch. Documentation goes to `docs`. Bugfixes in `bugfix`. 
The `dev` branch is meant for integration between the `feature` and the `bugfix` branch. Additional tests must be added to `testing/` branches.

```declarative
master <---- test <---- dev <---- feature <---- feat/
              |          |
              |          -------- bugfix <---- bug/
              |
              -- testing/
```

## Future Work
- Get rid of dangerous run-time reflection. Move to compile-time with macros instead. 
- Make code more generic allowing for TableParser to be configured for smaller dataframes and Spark for larger dataframes and distributed clusters.
- Allow for timeout duration for a job to be set.
- Support for more trigger types.
- Logging and Monitoring support with Prometheus.
- A sandbox mode for jobs.
- General improvements to code quality. 