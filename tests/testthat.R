library(testthat)
library(assertthat)
library(RJDBC)
library(dplyr)
library(dplyr.snowflakedb)
options(dplyr.jdbc.classpath = Sys.getenv("SNOWFLAKE_JAR"))

test_check("dplyr.snowflakedb")
