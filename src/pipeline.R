# =========================================================================
# PROJECT: Cloud-Ready Data Cleansing & Export Pipeline
# ARCHITECTURE: Functional, Parameterized, Production-Grade
# AUTHOR: Luiz Augusto Bentemuler Rodrigues 
# =========================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(wbstats)
  library(stringi)
  library(writexl) 
  library(zoo)
})

# -------------------------------------------------------------------------
# 1. PIPELINE INGESTION LAYER (EXTRACT)
# -------------------------------------------------------------------------

#' Fetch Valid Country Codes
#' @description Isolates metadata fetching to prevent aggregate bias and side-effects.
fetch_valid_countries <- function() {
  message("[INFO] Fetching country metadata...")
  tryCatch({
    wb_countries() %>% 
      filter(region != "Aggregates") %>% 
      pull(iso2c)
  }, error = function(e) {
    stop(sprintf("CRITICAL ERROR: Failed to fetch country metadata. Reason: %s", e$message))
  })
}

#' Resilient Ingestion with Exponential Backoff
#' @description Fetches indicators and logs actual system error messages.
extract_raw_data <- function(indicators, start, end, max_attempts = 3) {
  message("[INFO] Connecting to World Bank API...")
  attempt <- 1
  success <- FALSE
  df      <- NULL
  
  while(!success && attempt <= max_attempts) {
    df <- tryCatch({
      wb_data(indicator = indicators, start_date = start, end_date = end)
    }, error = function(e) {
      wait_time <- 2 ^ attempt
      message(sprintf("[WARNING] Attempt %d failed. Error: %s", attempt, str_trim(e$message)))
      message(sprintf("[WARNING] Reconnecting in %ds...", wait_time))
      Sys.sleep(wait_time)
      return(NULL)
    })
    if (!is.null(df)) success <- TRUE
    attempt <- attempt + 1
  }
  
  if (is.null(df)) stop("CRITICAL ERROR: World Bank API is completely unavailable.")
  return(df)
}

# -------------------------------------------------------------------------
# 2. DATA SANITIZATION LAYER (TRANSFORM) - Pure Function
# -------------------------------------------------------------------------

#' Safe Interpolation Wrapper
#' @description Prevents zoo::na.approx from crashing when a country has fewer than 2 valid points.
safe_interpolate <- function(x) {
  if (sum(!is.na(x)) > 1) {
    return(zoo::na.approx(x, na.rm = FALSE))
  }
  return(x)
}

#' Sanitize Economic Data
#' @description A pure transformation function without implicit global dependencies or network side-effects.
sanitize_economic_data <- function(raw_df, indicator_mapping, valid_countries) {
  message("[INFO] Initiating dataset sanitization and standardization...")
  
  initial_rows <- nrow(raw_df)
  
  # Step 1: Structural filtering and initial renaming
  staged_df <- raw_df %>%
    filter(iso2c %in% valid_countries) %>% 
    rename(year = date) %>% 
    rename(any_of(indicator_mapping)) %>% 
    distinct(country, year, .keep_all = TRUE)
  
  # Step 2: Safe Imputation (Linear interpolation bounded within each country)
  # FIX: total_population included to prevent accidental data loss in drop_na()
  imputed_df <- staged_df %>%
    group_by(country) %>%
    arrange(year, .by_group = TRUE) %>%
    mutate(
      gdp_per_capita   = safe_interpolate(gdp_per_capita),
      life_expectancy  = safe_interpolate(life_expectancy),
      total_population = safe_interpolate(total_population)
    ) %>%
    ungroup()
  
  # Step 3: Text Normalization and Final Schema Mapping
  clean_df <- imputed_df %>%
    # Drop rows where critical context couldn't be recovered
    drop_na(gdp_per_capita, life_expectancy, total_population) %>% 
    mutate(
      country_clean = stringi::stri_trans_general(country, "Latin-ASCII"), 
      country_clean = str_to_lower(country_clean),                        
      country_clean = str_trim(country_clean),
      reference_date = as.Date(paste0(year, "-12-31"))
    ) %>% 
    select(
      country_id = iso2c,
      country_name = country_clean,
      year,
      date_iso = reference_date,
      gdp_per_capita,
      life_expectancy,
      total_population
    )
  
  # Production Logs: Audit data loss
  final_rows <- nrow(clean_df)
  dropped_rows <- initial_rows - final_rows
  message(sprintf("[AUDIT] Row tracking: Initial = %d | Cleaned = %d | Dropped = %d", 
                  initial_rows, final_rows, dropped_rows))
  
  if (final_rows == 0) {
    stop("CRITICAL DATA ERROR: Sanitization pipeline resulted in 0 rows. Check API payload data.")
  }
  
  return(clean_df)
}

# -------------------------------------------------------------------------
# 3. ORCHESTRATION & DELIVERY LAYER (LOAD)
# -------------------------------------------------------------------------

#' Execute Master Pipeline
#' @description Fully parameterized orchestrator. No reliance on the global environment.
execute_pipeline <- function(indicators, start_year, end_year, output_base_name = "clean_worldbank_data") {
  
  # 1. Context Isolation (Extract)
  valid_countries <- fetch_valid_countries()
  raw_data        <- extract_raw_data(indicators, start_year, end_year)
  
  # 2. Data Transformation (Transform)
  final_data      <- sanitize_economic_data(raw_data, indicators, valid_countries)
  
  # 3. Local Persistence (Load)
  csv_path   <- paste0(output_base_name, ".csv")
  excel_path <- paste0(output_base_name, ".xlsx")
  
  write_excel_csv(final_data, csv_path)
  write_xlsx(final_data, excel_path)
  
  absolute_path <- normalizePath(".", winslash = "/")
  
  message(sprintf("[SUCCESS] Pipeline completed! %d rows successfully processed.", nrow(final_data)))
  message(sprintf("[FILE] Artifacts saved to:\n -> %s/%s\n -> %s/%s", 
                  absolute_path, csv_path, absolute_path, excel_path))
  
  return(invisible(final_data))
}

# -------------------------------------------------------------------------
# 4. RUNTIME EXECUTION
# -------------------------------------------------------------------------

TARGET_INDICATORS <- c(
  gdp_per_capita   = "NY.GDP.PCAP.CD", 
  life_expectancy  = "SP.DYN.LE00.IN", 
  total_population = "SP.POP.TOTL"
)

execute_pipeline(
  indicators = TARGET_INDICATORS, 
  start_year = 2010, 
  end_year   = 2025
)
