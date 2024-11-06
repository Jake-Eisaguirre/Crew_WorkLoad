
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc, writexl)


current_bid_period <- substr(as.character(Sys.Date()), 1, 7)

previous_bid_period <- (as.character(format(seq(Sys.Date(), by = "-6 month", length = 2)[2], "%Y-%m"))) 

previous_filt_bid_period <- (as.character(format(seq(Sys.Date(), by = "-5 month", length = 2)[2], "%Y-%m"))) 

fut_bid_period <- (as.character(format(seq(Sys.Date(), by = "1 month", length = 2)[2], "%Y-%m"))) 

# Connect to the `PLAYGROUND` database and append data if necessary
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),  # Establish a database connection using ODBC for the playground database
                                     Driver = "SnowflakeDSIIDriver",  # Specify the Snowflake ODBC driver
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",  # Server address
                                     WAREHOUSE = "DATA_LAKE_READER",  # Specify the Snowflake warehouse
                                     Database = "PLAYGROUND",  # Specify the database name
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # User ID for authentication
                                     authenticator = "externalbrowser")  # Use external browser for authentication
  print("Database Connected!")  # Print success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS") 


# Queries
reserve_q <- paste0("select * from AA_RESERVE_UTILIZATION WHERE BID_PERIOD BETWEEN '", previous_bid_period,"' AND '", fut_bid_period,"';")

raw_reserve <- dbGetQuery(db_connection_pg, reserve_q)

fp_q <- paste0("select* from AA_FINAL_PAIRING WHERE BID_PERIOD BETWEEN '", previous_bid_period,"' AND '", fut_bid_period,"';")

raw_fp <- dbGetQuery(db_connection_pg, fp_q)



clean_fp <- raw_fp %>% 
  select(FLIGHT_DATE, PAIRING_POSITION, BID_PERIOD, EQUIPMENT, CREW_ID, BASE) %>% 
  filter(!EQUIPMENT == "33Y",
         #BID_PERIOD >= prev_bid_period
  ) %>% 
  mutate(PAIRING_POSITION = ifelse(PAIRING_POSITION %in% c("CA", "FO", "RO", "FA"),
                                   PAIRING_POSITION, 
                                   "FA"),
         EQUIPMENT = if_else(PAIRING_POSITION == "FA", NA, EQUIPMENT),
         BASE = if_else(BASE == "HAL", "HNL", BASE),
         PAIRING_POSITION = if_else(PAIRING_POSITION == "RO", "FO", PAIRING_POSITION)) %>%
  group_by(FLIGHT_DATE, PAIRING_POSITION, EQUIPMENT, BASE) %>% 
  reframe(#BID_PERIOD,
          n_bid = n_distinct(CREW_ID)) %>% 
  distinct()

# t <- clean_fp %>% 
#   filter(FLIGHT_DATE == "2024-08-31",
#          PAIRING_POSITION == "FA") %>% 
#   mutate(n = length(unique(CREW_ID)))

clean_reserve <- raw_reserve %>% 
  filter(TRANSACTION_CODE %in% c("RLV", "SCR", "ARC", "RSV"),
         #BID_PERIOD >= prev_bid_period
  ) %>% 
  mutate(PAIRING_POSITION = ifelse(PAIRING_POSITION %in% c("CA", "FO", "RO", "FA"),
                                   PAIRING_POSITION, 
                                   "FA"),
         BASE = if_else(BASE == "HAL", "HNL", BASE),
         PAIRING_POSITION = if_else(PAIRING_POSITION == "RO", "FO", PAIRING_POSITION)) %>% 
  group_by(PAIRING_POSITION, PAIRING_DATE, EQUIPMENT, BASE) %>% 
  reframe(BID_PERIOD,
          n_reserve = n()) %>% 
  mutate(EQUIPMENT = if_else(EQUIPMENT == "NA", NA, EQUIPMENT)) %>% 
  rename(FLIGHT_DATE = PAIRING_DATE) %>% 
  distinct()



combined_gold <- inner_join(clean_fp, clean_reserve) %>% 
  mutate(workload = round((n_reserve/n_bid),2)) %>% 
  filter(BID_PERIOD >= previous_filt_bid_period) %>% 
  mutate(BID_PERIOD = as.factor(BID_PERIOD)) %>% 
  mutate(PAIRING_POSITION = if_else(BASE == "LAX" & PAIRING_POSITION == "FA", "FA-LAX", PAIRING_POSITION),
         PAIRING_POSITION = if_else(BASE == "HNL" & PAIRING_POSITION == "FA", "FA-HNL", PAIRING_POSITION))

write_xlsx(combined_gold, "Z:/OperationsResourcePlanningAnalysis/Work_Load/workload_data.xlsx")

print("Data Uploaded")
Sys.sleep(10)
