#' @import RJDBC
#' @import dplyr
NULL

#' dplyr backend support for SnowflakeDB (https://snowflake.net)
#'
#' Use \code{src_snowflakedb} to connect to an existing Snowflake database,
#' and \code{tbl} to connect to tables within that database.
#'
#' @template db-info
#' @param user Username
#' @param password Password
#' @param account Account Name (e.g. <account>.snowflakecomputing.com)
#' @param host Hostname (Not required for public endpoints, defaults to
#'             <account>.snowflakecomputing.com)
#' @param port Port (Defaults to 443, the default for public endpoints)
#' @param opts List of other parameters to pass (warehouse, db, schema, tracing)
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
                            opts=list(), ...) {
  requireNamespace("RJDBC", quietly = TRUE)
  requireNamespace("dplyr", quietly = TRUE)

  if (length(names(opts)) > 0) {
    opts <- paste0("&",
                   paste(lapply(names(opts),
                   function(x){paste(x,opts[x], sep="=")}),
                   collapse="&"))
  }
  else {
    opts <- ""
  }

  if (is.null(host)) {
    host = paste0(account, ".snowflakecomputing.com")
  }
  url <- paste0("jdbc:snowflake://", host, ":", as.character(port),
                "/?account=", account, opts)
  message("URL: ", url)
  con <- dbConnect(RJDBC::JDBC(driverClass = "com.snowflake.client.jdbc.SnowflakeDriver",
                               classPath = getOption('dplyr.jdbc.classpath', NULL),
                               identifier.quote = "\""),
                   url,
                   user,
                   password, ...)
  res <- dbGetQuery(con, 'SELECT
                          CURRENT_USER() AS USER,
                          CURRENT_DATABASE() AS DBNAME,
                          CURRENT_VERSION() AS VERSION,
                          CURRENT_SESSION() AS SESSIONID')
  info <- list(dbname = res$DBNAME, url = url,
               version = res$VERSION, user = res$USER, Id = res$SESSIONID)

  env <- environment()
  # temporarily suppress the warning messages from getPackageName()
  wmsg <- getOption('warn')
  options(warn = -1)
  SnowflakeDBConnection <- methods::setRefClass("SnowflakeDBConnection",
                           contains = c("JDBCConnection"), where = env)
  options(warn = wmsg)
  con <- structure(con, class = c("SnowflakeDBConnection", "JDBCConnection"))

  # Creates an environment that disconnects the database when it's
  # garbage collected
  db_disconnector <- function(con, name, quiet = FALSE) {
    reg.finalizer(environment(), function(...) {
      if (!quiet) {
        message("Auto-disconnecting ", name, " connection ",
                "(", paste(con@Id, collapse = ", "), ")")
      }
      dbDisconnect(con)
    })
    environment()
  }

  dplyr::src_sql("snowflakedb", con, info = info,
          disco = db_disconnector(con, "snowflakedb"))
}

#' @export
#' @rdname src_snowflakedb
tbl.src_snowflakedb <- function(src, from, ...) {
  dplyr::tbl_sql("snowflakedb", src = src, from = from, ...)
}

#' @export
src_desc.src_snowflakedb <- function(x) {
  info <- x$info
  paste0("SnowflakeDB: ", info$version, "\nURL: ", info$url, "\n")
}

#' @export
src_translate_env.src_snowflakedb <- function(x) {
  dplyr::sql_variant(
    dplyr::base_scalar,
    dplyr::sql_translator(.parent = dplyr::base_agg,
      n = function() dplyr::sql("COUNT(*)"),
      cor = dplyr::sql_prefix("CORR"),
      cov = dplyr::sql_prefix("COVAR_SAMP"),
      sd =  dplyr::sql_prefix("STDDEV_SAMP"),
      var = dplyr::sql_prefix("VAR_SAMP"),
      # all = dplyr::sql_prefix("bool_and"),
      # any = dplyr::sql_prefix("bool_or"),
      n_distinct = function(x) dplyr::build_sql("COUNT(DISTINCT ", x, ")"),
      paste = function(x, collapse) dplyr::build_sql("LISTAGG(", x, collapse, ")")
    ),
    base_win
  )
}

# DBI methods ------------------------------------------------------------------

#' @export
db_analyze.SnowflakeDBConnection <- function(con, table) {
  # SnowflakeDB has no ANALYZE command so just return TRUE if called
  return(TRUE)
}

#' @export
db_create_index.SnowflakeDBConnection <- function(con, table, columns, name = NULL, ...) {
  # SnowflakeDB has no CREATE INDEX command so just return TRUE if called
  return(TRUE)
}

#' @export
db_begin.SnowflakeDBConnection <- function(con, ...) {
  dbSendQuery(con, "BEGIN TRANSACTION")
}

#' @export
db_query_fields.SnowflakeDBConnection <- function(con, query, ...) {
  # this fails when only a table name is passed in because it is single quoted
  # using ident() will add a second double qoting when it is not necessary
  s <- dplyr::build_sql("SELECT * FROM ", query, " LIMIT 0", con = con)
  if (isTRUE(getOption("dplyr.show_sql"))) message("SQL: ", s)
  names(dbGetQuery(con, s))
}

# db_insert_into.SnowflakeDBConnection <- function(con, table, values, ...) {
#   # copied from PostgreSQLConnection
#   cols <- lapply(values, escape, collapse = NULL, parens = FALSE, con = con)
#   col_mat <- matrix(unlist(cols, use.names = FALSE), nrow = nrow(values))

#   rows <- apply(col_mat, 1, paste0, collapse = ", ")
#   values <- paste0("(", rows, ")", collapse = "\n, ")

#   sql <- build_sql("INSERT INTO ", ident(table), " VALUES ", sql(values))
#   dbGetQuery(con, sql)
# }

#' @export
db_insert_into.SnowflakeDBConnection <- function(con, table, values, ...) {
  table

  # Convert factors to strings
  is_factor <- vapply(values, is.factor, logical(1))
  values[is_factor] <- lapply(values[is_factor], as.character)

  # Encode special characters in strings
  is_char <- vapply(values, is.character, logical(1))
  values[is_char] <- lapply(values[is_char], encodeString)

  # write the table out to a local tsv file
  tmp <- tempfile(fileext = ".tsv")
  write.table(values, tmp, sep = "\t", quote = FALSE, row.names = FALSE, col.names = TRUE, na = "")

  # put the tsv file to the Snowflake table stage
  sql <- sprintf("PUT file://%s @%%\"%s\"", tmp, table)
  message(sql)
  rs <- dbGetQuery(con, sql)
  if (rs["status"] != "UPLOADED") print(rs)

  # load the file from the table stage
  sql <- dplyr::build_sql("COPY INTO ", ident(table), " FILE_FORMAT = (FIELD_DELIMITER = '\\t' SKIP_HEADER = 1)")
  message(sql)
  rs <- dbGetQuery(con, sql)
  if (rs["errors_seen"] != "0") print(rs)

}

#' @export
db_data_type.SnowflakeDBConnection <- function(con, fields) {
  data_type <- function(x) {
    switch(class(x)[1],
      # logical = "BOOLEAN",
      logical = "VARCHAR(1)",
      integer = "NUMBER(10)",
      numeric = "DOUBLE",
      factor =  "VARCHAR",
      character = "VARCHAR",
      Date = "DATE",
      POSIXct = "TIMESTAMP",
      stop("Can't map type ", paste(class(x), collapse = "/"),
           " to a supported database type.")
    )
  }
  vapply(fields, data_type, character(1))
}

#' @export
db_explain.SnowflakeDBConnection <- function(con, sql, ...) {
  message("explain() not supported for SnowflakeDB")
}
#
# # temporary menthods for right/full join
# # https://github.com/NikNakk/dplyr/commit/7042e824ed8b6e2f33a9af7a48998d6cbd46d101
#
# #' @export
# right_join.tbl_snowflakedb <- function(x, y, by = NULL, copy = FALSE,
#                               auto_index = FALSE, ...) {
#   by <- dplyr::common_by(by, x, y)
#   y <- dplyr::auto_copy(x, y, copy, indexes = if (auto_index) list(by$y))
#   sql <- dplyr::sql_join(x$src$con, x, y, type = "right", by = by)
#   dplyr::update(tbl(x$src, sql), group_by = groups(x))
# }
#
# #' @export
# full_join.tbl_snowflakedb <- function(x, y, by = NULL, copy = FALSE,
#                               auto_index = FALSE, ...) {
#   by <- dplyr::common_by(by, x, y)
#   y <- dplyr::auto_copy(x, y, copy, indexes = if (auto_index) list(by$y))
#   sql <- dplyr::sql_join(x$src$con, x, y, type = "left", by = by)
#   dplyr::update(tbl(x$src, sql), group_by = groups(x))
# }
