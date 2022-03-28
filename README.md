<!--
Copyright 2017 Snowflake Computing Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->



## NOTE

This package is no longer being maintained. RStudio has added support for Snowflake in their odbc and dplyr packages. 

# dplyr.snowflakedb

This R package extends [`dplyr`](https://github.com/hadley/dplyr) to add backend support for [SnowflakeDB](https://snowflake.net).  More details on `dplyr` can be found in the [README](https://github.com/hadley/dplyr/blob/master/README.md) page for the project.

## R Environment Setup

The `dplyr.snowflakedb` connectivity to SnowflakeDB uses the `RJDBC` package, however,  the `rJava` package needs to be installed and working with Java 8 to support the SnowflakeDB JDBC requirements.  This may require:

* installing Java 8
* running `R CMD javareconf` so R uses the Java 8 for its `JAVA_HOME`
* installing `rJava` from source so it can be linked against Java 8

If you are using R on Mac OS X, please see [this wiki page](https://github.com/snowflakedb/dplyr-snowflakedb/wiki/Configuring-R-rJava-RJDBC-on-Mac-OS-X) for the necessary installation steps for `rJava`.

Once you have `rJava` installed and verified it is using Java 8, you can install `dplyr` and dependencies.

```R
install.packages(c("RJDBC", "DBI", "dplyr"))
```

If you'd like some data to experiment with I'd recommend installing the following packages that most of the `dplyr` examples and vignettes use.

```R
install.packages(c("nycflights13", "Lahman"))
```

## Installing dplyr.snowflakedb

```R
install.packages("devtools")
devtools::install_github("snowflakedb/dplyr-snowflakedb")
```

## Example SnowflakeDB Connection

```R
library(dplyr)
library(dplyr.snowflakedb)
options(dplyr.jdbc.classpath = "/home/snowman/Downloads/snowflake_jdbc.jar")

my_db <- src_snowflakedb(user = "snowman",
                         password = "letitsnow",
                         account = "acme",
                         opts = list(warehouse = "mywh",
                                     db = "mydb",
                                     schema = "public"))
```

##  Issues

Please file any issues or bugs you find using the project's [issue page](https://github.com/snowflakedb/dplyr-snowflakedb/issues).  Please include a minimal reproducible example where possible.

## Contributing

If you find an issue and would like to fix it yourself, please do, and submit a [pull request](https://help.github.com/articles/using-pull-requests/) so it can be reviewed and merged.

## Copyright and License

Copyright 2017 Snowflake Computing, Inc. Licensed under the [Apache License, Version 2.0](https://github.com/snowflakedb/dplyr-snowflakedb/blob/master/LICENSE).
