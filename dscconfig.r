################################################################
# Data Science Challenge - Configuration File
################################################################
# Configuration file containing global values and helper functions

suppressMessages(library(RSQLite))
suppressMessages(library(DBI))
suppressMessages(library(dplyr))
suppressMessages(library(reshape2))
suppressMessages(library(ggplot2))
suppressMessages(library(scales))
# suppressMessages(library(httr))
# The library log4r is used directly. It is required, but not loaded.

########## Global constants and settings ##########
DATA_FOLDER = "Data"               # data folder name
FILE_LOG = "dscchllg"              # Log file name, to be created/appended at the project root folder.
DATE_LIMIT = "2015-04-01 00:00:00"


########## Logging System Initialization ##########
# Logging, warning and error object creation and control
logger <- log4r::create.logger()
log4r::logfile(logger) <- file.path(paste("./", FILE_LOG, ".log", sep = ""))
log4r::level(logger) <- "INFO"

# Wrapper functions to write the log and screen progress
# All methods are called as log4r::XXXX to avoid loading the package and the
# potential "debug" function conflict.
log_new_error <- function(error_message) {
  log4r::error(logger, error_message)
  # execution_errors[[length(execution_errors) + 1]] <<- error_message
  print(paste("ERROR", error_message))
}

log_new_warning <- function(warning_message) {
  log4r::warn(logger, warning_message)
  # execution_warnings[[length(execution_warnings) + 1]] <<- warning_message
  print(paste("WARNING", warning_message))
}

log_new_info <- function(info_message) {
  log4r::info(logger, info_message)
  print(paste("INFO", info_message))
}

log_new_debug <- function(debug_message) {
  log4r::debug(logger, debug_message)
  print(paste("DEBUG", debug_message))
}

