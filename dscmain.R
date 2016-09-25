################################################################
# Data Science Challenge - Main Script
################################################################
# Script to be executed sequentially to process the provided data and
# execute the necessary techniques to answer the business questions.
# The script is organized in different sections to be executed as needed
# and avoid re-processing long-running operations

source("./dscconfig.r")
source("./dscdatafunc.r")

########## Execution Sequence (enable/disable) ##########
EXECUTE_LOAD_RAW_DATA <- FALSE
EXECUTE_DATA_CLEANING <- FALSE
EXECUTE_SAVE_CLEAN_DATA <- FALSE
EXECUTE_LOAD_CLEAN_DATA <- FALSE
EXECUTE_DATA_TRANSFORMATION <- FALSE
EXECUTE_SAVE_TRANSFORMED_DATA <- FALSE
EXECUTE_LOAD_TRANSFORMED_DATA <- TRUE
EXECUTE_GENERATE_PLOTS <- FALSE

# Process logging start
sstart_time <- proc.time()
log_new_info("***** MAIN_EXECUTION START *****")


########## Load Raw Data ##########
if(EXECUTE_LOAD_RAW_DATA){
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Raw_Data")
  
  cd_conn = dbConnect(RSQLite::SQLite(), dbname = "./data/contentDiscovery.db")
  
  # Articles are de-normalized by combining the topic and type values from their respective tables
  articles <- dbGetQuery(cd_conn, "select ar.article_id, ar.author_id, ar.topic_id, tc.name as topic_name, ar.type_id, ty.name as type_name, ar.submission_time
                         from articles as ar, topics as tc, types as ty
                         where ar.topic_id == tc.topic_id and ar.type_id == ty.type_id")
  
  # Content and Users tables are read directly with joins required
  content <- dbGetQuery(cd_conn, "select * from email_content")
  
  users <- dbGetQuery(cd_conn, "select * from users")
  
  # Web Log is read directly to later extract the values from the request field
  access <- read.csv("./data/access.log", sep = " ", col.names = c("timestamp", "request", "httpstatus", "bytessent"), stringsAsFactors = FALSE)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Raw_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Raw_Data")
}


########## Process Data Cleaning ##########
if(EXECUTE_DATA_CLEANING) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Process_Data_Cleaning")
  
  articles <- clean_articles(articles)
  content <- clean_content(content)
  users <- clean_users(users)
  access <- clean_access(access)

  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Process_Data_Cleaning - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Process_Data_Cleaning")
}


########## Save Clean Data ##########
if(EXECUTE_SAVE_CLEAN_DATA) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Save_Clean_Data")
  
  obj_to_save <- c("access", "articles", "content", "users")
  save_data_file(obj_to_save, "cleaned_data")
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Save_Clean_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Save_Clean_Data")
}


########## Load Clean Data ##########
if(EXECUTE_LOAD_CLEAN_DATA){
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Clean_Data")
  
  load_data_file("cleaned_data")
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Clean_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Clean_Data")
}


########## Process Data Transformation ##########
if(EXECUTE_DATA_TRANSFORMATION) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Process_Data_Transformation")
  
  articles <- transform_articles(articles)
  content <- transform_content(content)
  topic_facts <- transform_topic_facts(articles)
  type_facts <- transform_type_facts(articles)
  users <- transform_users(users)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Process_Data_Transformation - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Process_Data_Transformation")
}

########## Save Transformed Data ##########
if(EXECUTE_SAVE_TRANSFORMED_DATA) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Save_Transformed_Data")
  
  obj_to_save <- c("access", "articles", "content", "topic_facts", "type_facts", "users")
  save_data_file(obj_to_save, "transformed_data")
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Save_Transformed_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Save_Transformed_Data")
}


########## Load Transformed Data ##########
if(EXECUTE_LOAD_TRANSFORMED_DATA){
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Transformed_Data")
  
  load_data_file("transformed_data")
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Transformed_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Transformed_Data")
}


########## Generate Plots ##########
if(EXECUTE_GENERATE_PLOTS){
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Generate_Plots")
  
  generate_type_topic_heatmap()
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Generate_Plots - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Generate_Plots")
}


########## Script Final Operations ##########
sstop_time <- proc.time() - sstart_time
log_new_info(paste("***** MAIN_EXECUTION END ***** - Elapsed:", sstop_time[3], "s"))
rm(cstart_time)
rm(cstop_time)
rm(sstart_time)
rm(sstop_time)