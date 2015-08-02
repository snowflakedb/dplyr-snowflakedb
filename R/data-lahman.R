#' @import dplyr
NULL

#' Cache and retrieve an \code{src_snowflakedb} of the Lahman baseball database.
#'
#' This creates an interesting database using data from the Lahman baseball
#' data source, provided by Sean Lahman at
#' \url{http://www.seanlahman.com/baseball-archive/statistics/}, and
#' made easily available in R through the \pkg{Lahman} package by
#' Michael Friendly, Dennis Murphy and Martin Monkman. See the documentation
#' for that package for documentation of the inidividual tables.
#'
#' @name lahman
NULL

#' Lahman function for SnowflakeDB.
#' @export
#' @param ... Parameters to pass through for src_snowflakedb()
lahman_snowflakedb <- function(...) {
  copy_lahman(src_snowflakedb(...))
}
