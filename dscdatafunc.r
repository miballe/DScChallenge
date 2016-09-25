################################################################
# Data Science Challenge - Data Processing Functions
################################################################
# Script that contains the functions used to manipulate the data
# with the propose to clean, transform or model

########## Cleaning Functions ##########

# Apply the necessary actions to have consistent data for the articles table
clean_articles <- function(df_articles) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_articles")
  
  df_articles$topic_name <- factor(df_articles$topic_name)
  df_articles$type_name <- factor(df_articles$type_name)
  # It seems tha the time zone doesn't have a practical effect for this scenario. Leaving all times as UTC for the moment
  df_articles$submission_time <- as.POSIXct(strptime(df_articles$submission_time, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_articles - Elapsed:", fstop_time[3], "s"))
  return(df_articles)
}

# Apply the necessary actions to have consistent data for the content table
clean_content <- function(df_content) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_content")
  
  df_content$send_time <- as.POSIXct(strptime(df_content$send_time, "%Y-%m-%d %H:%M:%S", tz = "UTC"))  
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_content - Elapsed:", fstop_time[3], "s"))
  return(df_content)
}

# Apply the necessary actions to have consistent data for the users table
clean_users <- function(df_users) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_users")
  
  # No operations required
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_users - Elapsed:", fstop_time[3], "s"))
  return(df_users)
}

# Apply the necessary actions to have consistent data for the access table
clean_access <- function(df_access) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_access")
  
  df_access$timestamp <- as.POSIXct(strptime(df_access$timestamp, "[%d/%b/%Y:%H:%M:%S]", tz = "UTC"))  
  df_access$httpstatus <- factor(df_access$httpstatus)
  
  # This code block breaks the request string assumming that article_id and user_id are always
  # present, and both follow the previously described order. This is true for the provided data
  # however, the right way to perform this operation is recognizing the query string variables
  # and filling the empty values if necessary. It was attempted, but the operation was very
  # expensive when using httr and parse_url. This alternative code was implemented taking
  # advantage that the provided data always meet the above condition.
  qstemp <- data.frame(sapply(strsplit(df_access$request, " "), "[", 2))
  names(qstemp) <- c("value")
  qstemp$value <- as.character(qstemp$value)
  qssplit <- strsplit(qstemp$value, "=")
  qsarticle <- data.frame(sapply(qssplit, "[[", 2))
  names(qsarticle) <- c("value")
  qsarticle$value <- as.character(qsarticle$value)
  qsuser <- data.frame(sapply(qssplit, "[[", 3))
  names(qsuser) <- c("value")
  qsuser$value <- as.character(qsuser$value)
  qssplit <- strsplit(qsarticle$value, "&")
  qsarticle <- data.frame(sapply(qssplit, "[[", 1))
  names(qsarticle) <- c("value")
  qsarticle$value <- as.character(qsarticle$value)
  df_access$article_id <- as.integer(qsarticle$value)
  df_access$user_id <- as.integer(qsuser$value)
  
  # This code block implements httr and parse_url, but those oeprations
  # are very time consuming when running.
    # qstemp$value <- paste("http://dummy.url", qstemp$value, sep = "")
    # qslist <- sapply(qstemp$value, FUN =  function(u) {
    #   x <- parse_url(u)
    #   x$query$article_id
    # })
    # qsvalues <- data.frame(qslist)
    # df_access$article_id <- qsvalues$qslist
    # qslist <- sapply(qstemp$value, FUN =  function(u) {
    #   x <- parse_url(u)
    #   x$query$user_id
    # })
    # qsvalues <- data.frame(qslist)
    # df_access$user_id <- qsvalues$qslist
  
  df_access <- df_access[df_access$timestamp < as.POSIXct(DATE_LIMIT) ,]
  df_access <- df_access[df_access$article_id <= max(articles$article_id),]
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_access - Elapsed:", fstop_time[3], "s"))
  return(df_access)
}

# Saves the cleaned data to an R file
save_data_file <- function(objects, file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - save_data_file")
  
  save(list = objects, file = paste("./", DATA_FOLDER, "/", file_name, ".RData", sep = ""))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - save_data_file - Elapsed:", fstop_time[3], "s"))
}

# Loads the clean data from an R file
load_data_file <- function(file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - load_data_file")
  
  load( file = paste("./", DATA_FOLDER, "/", file_name, ".RData", sep = ""), envir = .GlobalEnv )
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - load_data_file - Elapsed:", fstop_time[3], "s"))
}

# Transforms the originally created articles table by adding the count of links and clicks
# associated in the observed activity. Then, the proportion of clicks vs links is calculated
# to have a unique comparison criteria. At the end the week number is also calculated to
# analyze the evolution during the analyzed period
transform_articles <- function(df_articles) {
  fstart_time <- proc.time()
  log_new_info("- START - transform_articles")
  
  article_links <- select(content, article_id) %>%
                   group_by(article_id) %>%
                   summarize(nlinks = n())
  df_articles <- merge(df_articles, article_links, "article_id", all.x = TRUE)
  df_articles[is.na(df_articles[,c("nlinks")]), c("nlinks")] <- 0
  
  article_clicks <- select(access, article_id) %>%
                    group_by(article_id) %>%
                    summarize(nclicks = n())
  df_articles <- merge(df_articles, article_clicks, "article_id", all.x = TRUE)
  df_articles[is.na(df_articles[,c("nclicks")]), c("nclicks")] <- 0
  
  df_articles$click_rate <- df_articles$nclicks / df_articles$nlinks
  # 1 is added to the week number to avoid the week 0 which is not intuitive for business users
  df_articles$week_number <- as.integer(strftime(df_articles$submission_time,format="%W")) + 1
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_articles - Elapsed:", fstop_time[3], "s"))
  return(df_articles)
}

# Transforms the originally created content table by adding the topic and type names
# with the aim to ease further aggregation operations.
transform_content <- function(df_content) {
  fstart_time <- proc.time()
  log_new_info("- START - transform_content")
  
  df_content <- merge(df_content, select(articles, article_id, topic_name, type_name), "article_id", all.x = TRUE)
  # df_content[is.na(df_content[,c("topic_name")]), c("topic_name")] <- "Unknown"
  # df_content[is.na(df_content[,c("type_name")]), c("type_name")] <- "Unknown"
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_content - Elapsed:", fstop_time[3], "s"))
  return(df_content)
}

transform_users <- function(df_users) {
  fstart_time <- proc.time()
  log_new_info("- START - transform_users")
  
  domaintemp <- data.frame(sapply(strsplit(df_users$email, "@"), "[", 2))
  names(domaintemp) <- c("domain")
  df_users$domain <- domaintemp$domain
  
  article_links <- select(content, user_id) %>%
    group_by(user_id) %>%
    summarize(nlinks = n())
  df_users <- merge(df_users, article_links, "user_id", all.x = TRUE)
  df_users[is.na(df_users[,c("nlinks")]), c("nlinks")] <- 0
  
  article_clicks <- select(access, user_id) %>%
    group_by(user_id) %>%
    summarize(nclicks = n())
  df_users <- merge(df_users, article_clicks, "user_id", all.x = TRUE)
  df_users[is.na(df_users[,c("nclicks")]), c("nclicks")] <- 0
  
  df_users$click_rate <- df_users$nclicks / df_users$nlinks
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_users - Elapsed:", fstop_time[3], "s"))
  return(df_users)
}

# This topic facts table consolidates the main click and links statistics per
# topic.
transform_topic_facts <- function(df_articles) {
  fstart_time <- proc.time()
  log_new_info("- START - transform_topic_facts")
  
  topic_facts <- select(df_articles, topic_name, nclicks, nlinks) %>%
                 group_by(topic_name) %>%
                 summarize(topic_clicks = sum(nclicks), topic_links = sum(nlinks))
  topic_facts$click_rate <- topic_facts$topic_clicks / topic_facts$topic_links
  topic_facts <- arrange(topic_facts, -click_rate)
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_topic_facts - Elapsed:", fstop_time[3], "s"))
  return(topic_facts)
}

# This content type facts table consolidates the main click and links
# statistics per topic.
transform_type_facts <- function(df_articles) {
  fstart_time <- proc.time()
  log_new_info("- START - transform_type_facts")
  
  type_facts <- select(df_articles, type_name, nclicks, nlinks) %>%
                group_by(type_name) %>%
                summarize(type_clicks = sum(nclicks), type_links = sum(nlinks))
  type_facts$click_rate <- type_facts$type_clicks / type_facts$type_links
  type_facts <- arrange(type_facts, -click_rate)
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_type_facts - Elapsed:", fstop_time[3], "s"))
  return(type_facts)
}

# This method generates a heatmap showing the relationship between topics and content types
# in the context of click rates.
generate_type_topic_heatmap <- function() {
  cc <- cast(articles, type_name ~ topic_name, value = "click_rate", fun.aggregate = mean)
  ccr <- melt(as.matrix(cc))
  p <- ggplot(ccr, aes(x = type_name, y = topic_name, fill = value)) +
       geom_tile() +
       labs(x = "Content Type", y = "Content Topic", title = "Topic ~ Type Heatmap") +
       scale_fill_gradient(low = "white", high = "steelblue") +
       theme(axis.text.x = element_text(angle = 60, hjust = 1))
  ggsave("./plot_heatmap_topic_type.png", plot = p, device = "png", width = 200, height = 400, units = "mm")
}

generate_users_click_hist <- function() {
  p <- ggplot(users, aes(x = click_rate)) +
       geom_histogram(binwidth = 30) +
       labs(x = "Click Rate", y = "Count", title = "User Click Rates")
  ggsave("./plot_hist_click_rate_user.png", plot = p, device = "png", width = 200, height = 200, units = "mm")
}
