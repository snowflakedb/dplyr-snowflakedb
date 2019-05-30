# Copyright 2017 Snowflake Computing Inc.
# (derived from dplyr, Copyright 2013-2015 RStudio)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' @import DBI
#' @import RJDBC
#' @import dplyr
NULL

#' dplyr backend support for SnowflakeDB (https://snowflake.net)
#'
#' Use \code{src_snowflakedb} to connect to an existing Snowflake database,
#' and \code{tbl} to connect to tables within that database.
#'

setClass(
  "SnowflakeDBConnection",
  representation = representation(
    con = "JDBCConnection",
    jc = "jobjRef",
    identifier.quote = "character",
    info = "list"
  ),
  contains = "JDBCConnection"
)

.SnowflakeDBConnection <- function(con, info) {
  new(
    "SnowflakeDBConnection",
    con = con,
    jc = con@jc,
    identifier.quote = con@identifier.quote,
    info = info
  )
}

#' @template db-info
#' @param user Username
#' @param password Password
#' @param account Account Name (e.g. <account>.snowflakecomputing.com)
#' @param host Hostname (Not required for public endpoints, defaults to
#'             <account>.snowflakecomputing.com)
#' @param port Port (Defaults to 443, the default for public endpoints)
#' @param opts List of other parameters to pass (warehouse, db, schema, tracing)
#' @param region_id Specifies the ID for the Snowflake Region where your account 
#' is located. (Default: us-west, example: us-east-1). 
#' See: \url{https://docs.snowflake.net/manuals/user-guide/intro-editions.html#region-ids-in-account-urls}
#' @param ... for the src, other arguments passed on to the underlying
#'   database connector, \code{dbConnect}. For the tbl, included for
#'   compatibility with the generic, but otherwise ignored.
#' @param src a snowflakedb src created with \code{src_snowflakedb}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @export
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # To connect to a database first create a src:
#' my_db <- src_snowflakedb(user = "snowman",
#'                          password = "letitsnow",
#'                          account = "acme",
#'                          opts = list(warehouse = "mywh",
#'                                      db = "mydb",
#'                                      schema = "public")
#' # Then reference a tbl within that src
#' my_tbl <- tbl(my_db, "my_table")
#' }
#'
#' \donttest{
#' # Here we'll use the Lahman database: to create your own in-database copy,
#' # create a database called "lahman", or tell lahman_snowflakedb() how to
#' # connect to a database that you can write to
#'
#' #if (has_lahman("snowflakedb", account = "acme",
#' #               user = "snowman", password = "letitsnow",
#' #               opts=list(warehouse="wh", db="lahman", schema="public"))) {
#' lahman_p <- lahman_snowflakedb()
#' # Methods -------------------------------------------------------------------
#' batting <- tbl(lahman_p, "Batting")
#' dim(batting)
#' colnames(batting)
#' head(batting)
#'
#' # Data manipulation verbs ---------------------------------------------------
#' filter(batting, yearID > 2005, G > 130)
#' select(batting, playerID:lgID)
#' arrange(batting, playerID, desc(yearID))
#' summarise(batting, G = mean(G), n = n())
#' mutate(batting, rbi2 = if(is.null(AB)) 1.0 * R / AB else 0)
#'
#' # note that all operations are lazy: they don't do anything until you
#' # request the data, either by `print()`ing it (which shows the first ten
#' # rows), by looking at the `head()`, or `collect()` the results locally.
#'
#' system.time(recent <- filter(batting, yearID > 2010))
#' system.time(collect(recent))
#'
#' # Group by operations -------------------------------------------------------
#' # To perform operations by group, create a grouped object with group_by
#' players <- group_by(batting, playerID)
#' group_size(players)
#'
#' summarise(players, mean_g = mean(G), best_ab = max(AB))
#' best_year <- filter(players, AB == max(AB) | G == max(G))
#' progress <- mutate(players,
#'   cyear = yearID - min(yearID) + 1,
#'   ab_rank = rank(desc(AB)),
#'   cumulative_ab = order_by(yearID, cumsum(AB)))
#'
#' # When you group by multiple level, each summarise peels off one level
#' per_year <- group_by(batting, playerID, yearID)
#' stints <- summarise(per_year, stints = max(stint))
#' filter(stints, stints > 3)
#' summarise(stints, max(stints))
#' # mutate(stints, order_by(yearID, cumsum(stints)))
#'
#' # Joins ---------------------------------------------------------------------
#' player_info <- select(tbl(lahman_p, "Master"), playerID, birthYear)
#' hof <- select(filter(tbl(lahman_p, "HallOfFame"), inducted == "Y"),
#'  playerID, votedBy, category)
#'
#' # Match players and their hall of fame data
#' inner_join(player_info, hof)
#' # Keep all players, match hof data where available
#' left_join(player_info, hof)
#' # Find only players in hof
#' semi_join(player_info, hof)
#' # Find players not in hof
#' anti_join(player_info, hof)
#'
#' # Arbitrary SQL -------------------------------------------------------------
#' # You can also provide sql as is, using the sql function:
#' batting2008 <- tbl(lahman_p,
#'   sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
#' batting2008
#' #}
#' }

src_snowflakedb <- function(user = NULL,
                            password = NULL,
                            account = NULL,
                            port = 443,
                            host = NULL,
                            opts = list(),
                            region_id = "us-west",
                            ...) {
  requireNamespace("RJDBC", quietly = TRUE)
  requireNamespace("dplyr", quietly = TRUE)
  
  valid_regions = c("us-east-1", "eu-central-1", "ap-southeast-2", "west-europe.azure")
  
  isWest <- substring(tolower(region_id), 1, 7) == "us-west"
  
  if (!isWest && !(tolower(region_id) %in% valid_regions))
    stop("Invalid 'region_id'. 
         See: https://docs.snowflake.net/manuals/user-guide/intro-editions.html#region-ids-in-account-urls")
  
  # set client metadata info
  snowflakeClientInfo <- paste0(
    '{',
    '"APPLICATION": "dplyr.snowflakedb",',
    '"dplyr.snowflakedb.version": "',
    packageVersion("dplyr.snowflakedb"),
    '",',
    '"dplyr.version": "',
    packageVersion("dplyr"),
    '",',
    '"R.version": "',
    R.Version()$version.string,
    '",',
    '"R.platform": "',
    R.Version()$platform,
    '"',
    '}'
  )
  
  # initalize the JVM and set the snowflake properties
  .jinit()
  .jcall(
    "java/lang/System",
    "S",
    "setProperty",
    "snowflake.client.info",
    snowflakeClientInfo
  )
  
  if (length(names(opts)) > 0) {
    opts <- paste0("&",
                   paste(lapply(names(opts),
                                function(x) {
                                  paste(x, opts[x], sep = "=")
                                }),
                         collapse = "&"))
  }
  else {
    opts <- ""
  }
  
  message("host: ", host)
  
  if (is.null(host) || host == "") {
    if (isWest)
      regionUrl <- ""
    else
      regionUrl <- paste0(".", tolower(region_id))
    
    host = paste0(account, regionUrl, ".snowflakecomputing.com")
  }
  
  url <- paste0("jdbc:snowflake://",
                host,
                ":",
                as.character(port),
                "/?account=",
                account,
                opts)
  message("URL: ", url)
  conn <-
    dbConnect(
      RJDBC::JDBC(
        driverClass = "com.snowflake.client.jdbc.SnowflakeDriver",
        classPath = getOption('dplyr.jdbc.classpath', NULL),
        identifier.quote = "\""
      ),
      url,
      user,
      password,
      ...
    )
  res <- dbGetQuery(
    conn,
    'SELECT
    CURRENT_USER() AS USER,
    CURRENT_DATABASE() AS DBNAME,
    CURRENT_VERSION() AS VERSION,
    CURRENT_SESSION() AS SESSIONID'
  )
  info <- list(
    dbname = res$DBNAME,
    url = url,
    version = res$VERSION,
    user = res$USER,
    Id = res$SESSIONID
  )

  env <- environment()
  # temporarily suppress the warning messages from getPackageName()
  wmsg <- getOption('warn')
  options(warn = -1)
  SnowflakeDBConnection <-
    methods::setRefClass(
      "SnowflakeDBConnection",
      fields = c("con", "jc", "identifier.quote", "info"),
      contains = c("JDBCConnection"),
      where = env
    )
  options(warn = wmsg)
  
  con <- structure(conn, info = info, class = c("SnowflakeDBConnection", "JDBCConnection"))
   
  # Creates an environment that disconnects the database when it's
  # garbage collected
  db_disconnector <- function(con, name, quiet = FALSE) {
    reg.finalizer(environment(), function(...) {
      if (!quiet) {
        message(
          "Auto-disconnecting ",
          name
        )
      }
      dbDisconnect(con)
    })
    environment()
  }
  
  dbplyr::src_sql("snowflakedb",
                  con,
                  disco = db_disconnector(con, "snowflakedb"))
}

#' @export
#' @rdname src_snowflakedb
tbl.src_snowflakedb <- function(src, from, ...) {
  dbplyr::tbl_sql("snowflakedb", src = src, from = from, ...)
}

#' @export
tbl.SnowflakeDBConnection <- function(con, from, ...) {
  src <- dbplyr::src_sql("snowflakedb", con)
  dbplyr::tbl_sql("snowflakedb", src = src, from = from, ...)
}

#' @export
db_desc.SnowflakeDBConnection <- function(x) {
  info <- x@info
  # Commented-out so that regression tests don't have to be updated with every new release. 
  # Instead we use a version and URL-agnostic printout. 
  
  # paste0("Snowflake Database: ", info$version, "\nURL: ", info$url, "\n")
  
  "SnowflakeDB Data Source"
}

#' @export
#sql_translate_env.src_snowflakedb <- function(x) {
sql_translate_env.SnowflakeDBConnection <- function(x) {
  dbplyr::sql_variant(
    dbplyr::base_scalar,
    dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function()
        dbplyr::sql("COUNT(*)"),
      cor = dbplyr::sql_prefix("CORR"),
      cov = dbplyr::sql_prefix("COVAR_SAMP"),
      sd =  dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      # all = dbplyr::sql_prefix("bool_and"),
      # any = dbplyr::sql_prefix("bool_or"),
      paste = function(x, collapse)
        dbplyr::build_sql("LISTAGG(", x, collapse, ")")
    ),
    dbplyr::sql_translator(
      .parent = dbplyr::base_win,
      n = function()
        dbplyr::sql("COUNT(*)"),
      cor = dbplyr::sql_prefix("CORR"),
      cov = dbplyr::sql_prefix("COVAR_SAMP"),
      sd =  dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      # all = dbplyr::sql_prefix("bool_and"),
      # any = dbplyr::sql_prefix("bool_or"),
      paste = function(x, collapse)
        dbplyr::build_sql("LISTAGG(", x, collapse, ")")
    )
  )
}

# DBI methods ------------------------------------------------------------------
#' @export
db_has_table.SnowflakeDBConnection <- function(con, table) {
  dbExistsTable(con, table) || dbExistsTable(con, toupper(table))
}

#' @export
db_analyze.SnowflakeDBConnection <- function(con, table) {
  # SnowflakeDB has no ANALYZE command so just return TRUE if called
  return(TRUE)
}

#' @export
db_create_index.SnowflakeDBConnection <-
  function(con, table, columns, name = NULL, ...) {
    # SnowflakeDB has no CREATE INDEX command so just return TRUE if called
    return(TRUE)
  }

#' @export
db_begin.SnowflakeDBConnection <- function(con, ...) {
  dbSendQuery(con, "BEGIN TRANSACTION")
}

#' @export
db_query_fields.SnowflakeDBConnection <- function(con, sql, ...) {
  fields <-
    dbplyr::build_sql("SELECT * FROM ", sql_subquery(con, sql), " LIMIT 0", con = con)
  if (isTRUE(getOption("dplyr.show_sql")))
    message("SQL: ", sql)
  names(dbGetQuery(con, fields))
}

#' @export
db_data_type.SnowflakeDBConnection <- function(con, fields) {
  data_type <- function(x) {
    switch(
      class(x)[1],
      logical = "BOOLEAN",
      integer = "NUMBER(10)",
      numeric = "DOUBLE",
      factor =  "VARCHAR",
      character = "VARCHAR",
      Date = "DATE",
      POSIXct = "TIMESTAMP",
      stop(
        "Can't map type ",
        paste(class(x), collapse = "/"),
        " to a supported database type."
      )
    )
  }
  vapply(fields, data_type, character(1))
}

#' @export
db_explain.SnowflakeDBConnection <- function(con, sql, ...) {
  message("explain() not supported for SnowflakeDB")
}

# Various helper methods ------------------------------------------------------------------

#' Perform a COPY INTO in Snowflake to perform a load or unload operation.
#' 
#' @param con A SnowflakeDBConnection object.
#' @param from The source of the data, i.e., what comes after the FROM in the COPY
#' statement. This can be a table, a subquery, a stage, or a local file.
#' @param to The target of the COPY statement as a string. This can be a S3/Azure/local
#' filesystem location, a table, or a Snowflake stage.
#' @param format_opts A list of key-value pairs for the Snowflake COPY file_format options.
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # To connect to a database first create a src:
#' my_db <- src_snowflakedb(user = "snowman",
#'                          password = "letitsnow",
#'                          account = "acme",
#'                          opts = list(warehouse = "mywh",
#'                                      db = "mydb",
#'                                      schema = "public")
#' # Copy from a local CSV file to a target table in my_db.
#' db_snowflake_copy(my_db$con, from = "file:///my/directory/foo.csv", to = "target_table", 
#' format_opts = list(format = 'csv', field_delimiter = ','))
#' }
#' @export
db_snowflake_copy <- function(con, from, to, format_opts = list()) {
  if (length(names(format_opts)) > 0) {
    opts <- paste0("file_format = (",
                   paste(lapply(names(format_opts),
                                function(x) {
                                  paste(x, format_opts[x], sep = "=")
                                }),
                         collapse = ", "), ")")
  }
  else
    opts <- ""
  
  rs <- dbGetQuery(con, paste("COPY INTO", to, "FROM", from, opts))
  
  if (rs["errors_seen"] != "0")
    warning(rs)
}

#' @export
db_put_to_stage <- function(con, file.name, stage.name) {
  rs <- dbGetQuery(con, paste0("PUT '", file.name, "'", stage.name))
  if (rs["status"] != "UPLOADED")
    warning(rs)
}

#' @export
db_get_from_stage <- function(con, file.name, stage.name) {
  dbGetQuery(con, paste0("GET ", stage.name, " '", file.name, "'"))
}

#' @export
db_create_stage <- function(con, stage.name, temporary = FALSE) {
  temp <- ifelse(temporary, "TEMP", "")
  dbSendQuery(con, paste("CREATE OR REPLACE", temp, "STAGE", stage.name))
}

db_load_from_file <- function(con, table.name, file.name) {
  stopifnot(file.exists(file.name))
  
  if (!db_has_table(con, table.name))
    stop("The specified table does not exist in Snowflake.")
  
  file.name <- paste0("file://", file.name)
  
  # Use table stage
  stage_name <-
    paste0("@%", id(table.name), "/dplyr_connector_staging_area/")
  db_put_to_stage(con, file.name, stage_name)

  db_snowflake_copy(
    con,
    stage_name,
    id(table.name),
    format_opts = list(
      field_delimiter = "'\\t'",
      skip_header = 1,
      null_if = "'NA'"
    )
  )
}

atomic_copy <- function(con, from, to) {
  temp_table_name <- paste0(to, "_dplyr_snowflakedb_temp_table")
  tryCatch({
    dbSendQuery(con,
                paste("CREATE OR REPLACE TABLE", id(temp_table_name), "LIKE", id(to)))
    db_load_from_file(con, temp_table_name, from)
    
    if (db_has_table(con, to))
      dbSendQuery(con,
                  paste("ALTER TABLE", id(to), "SWAP WITH", id(temp_table_name)))
    else
      dbSendQuery(con,
                  paste("ALTER TABLE", id(temp_table_name), "RENAME TO", id(to)))
  },
  # If PUT or COPY fail even partially, be sure to catch it and stop alteration of original table
  warning = function(w) {
    stop(w)
  },
  finally = {
   # dbSendQuery(con,
    #            paste("DROP TABLE IF EXISTS", temp_table_name))
  })
}

# Override copy_into and db_insert_into to use bulk COPY ------------------------------------------

#' @export
copy_to.src_snowflakedb <-
  function(dest,
           df,
           name = deparse(substitute(df)),
           overwrite = FALSE,
           mode = "safe",
           ...) {
    
    if (overwrite)
      mode = "overwrite"
    
    mode <- tolower(mode)
    stopifnot(is.data.frame(df),
              is.string(name),
              mode %in% c("safe", "overwrite", "append"))
    
    if(class(dest)[[1]] == "src_snowflakedb")
      connection <- dest$con
    else
      connection <- dest
    
    has_table <- db_has_table(connection, name)
    
    if (has_table) {
      message("The table ", name, " already exists.")
      if (mode != "overwrite" && mode != "append")
        stop(
          "Could not copy because the table already exists in the database.
          Set parameter 'mode' to either \"overwrite\"or \"append\"."
        )
    }
    
    types <- db_data_type(connection, df)
    names(types) <- names(df)
    
    tryCatch({
      tmpfilename = tempfile(fileext = ".tsv")

      #fix windows path issue in JDBC
      winStr <- "windows"
      if(grepl(winStr, tolower(.Platform["OS.type"]))) {
        tmpfilename <- gsub("\\\\","\\\\\\\\\\\\\\\\",tmpfilename)
      }

      write.table(
        df,
        file = tmpfilename,
        sep = "\t",
        row.names = FALSE,
        quote = FALSE
      )
      if (mode == "overwrite" || !has_table) {
        if (!has_table)
          db_create_table(connection, name, types)
        
        atomic_copy(connection, tmpfilename, name)
      }
      else
        db_load_from_file(connection, name, tmpfilename)
    },
    error = function(e) {
      if (!has_table && db_has_table(connection, name))
        db_drop_table(connection, name)
      
      stop(e)
    })
    
    tbl(connection, name)
  }

#' @export
copy_to.SnowflakeDBConnection <- copy_to.src_snowflakedb

#' @export
db_insert_into.SnowflakeDBConnection <-
  function(con, table, values, ...) {
    copy_to(
      dest = con,
      df = values,
      name = table,
      mode = "append",
      ...
    )
  }

id <- function(name) {
  dbplyr::sql_quote(name, '"')
}
# snowflake.identifier <- function(name) {
#   if (is.quoted(name)) 
#     name
#   else
#     paste0("\"", toupper(name), "\"")
# }
# 
# is.quoted <- function(string) {
#   # In R-3.4, can be replaced with base::startsWith and base::endsWith
#   #startsWith(string, "\"") && endsWith(string, "\"")
#    
#   substring(string, 1, 1) == "\"" && 
#     substring(string, nchar(string), nchar(string)) == "\""
# }

#' @export
setMethod("dbGetRowsAffected", signature("JDBCResult"), function(res) {
  0
})

#' #' @export
#' db_insert_into.SnowflakeDBConnection <-
#'   function(con, table, values, ...) {
#'     # write the table out to a local tsv file
#'     tmp <- tempfile(fileext = ".tsv")
#'     write.table(
#'       values,
#'       tmp,
#'       sep = "\t",
#'       quote = FALSE,
#'       row.names = FALSE,
#'       col.names = TRUE
#'     )
#'
#'     # put the tsv file to the Snowflake table stage
#'     message(sql)
#'     rs <- dbGetQuery(con, sql)
#'     if (rs["status"] != "UPLOADED")
#'       print(rs)
#'
#'     # load the file from the table stage
#'     sql <-
#'       dbplyr::build_sql(
#'         "COPY INTO ",
#'         ident(table),
#'         " FILE_FORMAT = (FIELD_DELIMITER = '\\t' SKIP_HEADER = 1 NULL_IF = 'NA')"
#'       )
#'     message(sql)
#'     rs <- dbGetQuery(con, sql)
#'     if (rs["errors_seen"] != "0")
#'       print(rs)
#'
#'   }
