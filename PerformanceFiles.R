#!/usr/bin/env Rscript

#' @author Carmelo Rodriguez Martinez \email{carmelor@@yyy-net.com}
#' @keywords CashBack
#' 
#' 
source("C:/Users/Administrator/Documents/scripts/setup_BI_DATA.R")

# log file
log_file <- "C:/Users/Administrator/Documents/scripts/logs/PerformanceFiles.R.log"

script_name <- "PerformanceFiles.R"
write_log("****************************************************************************************")
write_log(paste0("Starting script: ",basename(script_name)))

# Establish the connection
execution_time <- Sys.time()
write_log(paste0("Starting at ",execution_time))
write_log("****************************************************************************************")

# Define retry parameters
max_retries <- 4
retry_interval <- 5 * 60  # 5 minutes in seconds

retry_attempts <- 0
connection_successful <- FALSE

while (retry_attempts < max_retries && !connection_successful) {
  tryCatch({
    
    conn <- dbConnect(odbc::odbc(), .connection_string = conn_str)
    connection_successful <- TRUE  # Connection successful
    write_log("Successful connection.")
    
    ## Delete all past files
    
    # List all files in the folder (non-recursive, just in aiPerformanceFiles/)
    all_files <- list.files("aiPerformanceFiles", full.names = TRUE)
    
    # Extract just the file names (without path)
    file_names <- basename(all_files)
    
    # Try to parse the starting part of each file name as a date (YYYY-MM-DD)
    file_dates <- as.Date(substr(file_names, 1, 10), format = "%Y-%m-%d")
    
    # Identify files with valid dates that are strictly before today
    past_files <- all_files[!is.na(file_dates) & file_dates < Sys.Date()]
    
    # Delete silently (suppress TRUE/FALSE output)
    if (length(past_files) > 0) {
      invisible(file.remove(past_files))
    }
    
    
    # Define yesterday
    yesterday <- Sys.Date() - 1
    
    # First day of the month (based on yesterday)
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    
    #####################################################################
    ############################ Main Targets ###########################
    #####################################################################
    
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],

        -- 3) Sum all metrics
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
    GROUP BY
        b.[Date]
    "
    
    df <- dbGetQuery(conn, query)
    
    df_main <- bind_rows(
      # Daily
      df %>%
        filter(Date == yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    # Add margin
    df_main$Margin <- round((df_main$GGR / df_main$STAKE) * 100, digits = 1)
    
    # Add market
    df_main$Market <-"ALL"
    
    # Same by markets
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],
        us.COUNTRY AS Market,
        
        -- 3) Sum all metrics
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date], us.COUNTRY
    "
    
    df_markets <- dbGetQuery(conn, query)
    df_main_markets <- bind_rows(
      # Daily
      df_markets %>%
        filter(Date == yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df_markets %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df_markets %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df_markets %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    
    # Add margin
    df_main_markets$Margin <- round((df_main_markets$GGR / df_main_markets$STAKE) * 100, digits = 1)
    
    # Other markets
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],

        -- 3) Sum all metrics
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
        AND us.COUNTRY NOT IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date]
    "
    
    df_others <- dbGetQuery(conn, query)
    
    df_main_others <- bind_rows(
      # Daily
      df_others %>%
        filter(Date == yesterday) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df_others %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df_others %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df_others %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    
    # Add margin
    df_main_others$Margin <- round((df_main_others$GGR / df_main_others$STAKE) * 100, digits = 1)
    
    # Add market
    df_main_others$Market <-"Others"

    # Brands
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],
        m.Market,
        SUM(b.DEPOSIT_AMOUNT) AS DEPs,
        SUM(b.Total_STAKE)    AS STAKE,
        SUM(b.Total_GGR)      AS GGR,
        SUM(b.Total_NGR)      AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users AS us
      ON us.GL_ACCOUNT = b.PARTYID
    CROSS APPLY (
        VALUES (
            CASE
                WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                WHEN us.BRANDID = 6                           THEN 'BET'
                ELSE NULL
            END
        )
    ) AS m(Market)
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
        AND m.Market IS NOT NULL         -- drop rows not in GCC or BET
    GROUP BY
        b.[Date],
        m.Market
    "
    
    df_brands <- dbGetQuery(conn, query)
    
    df_main_brands <- bind_rows(
      # Daily
      df_brands %>%
        filter(Date == yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df_brands %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df_brands %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df_brands %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    
    # Add margin
    df_main_brands$Margin <- round((df_main_brands$GGR / df_main_brands$STAKE) * 100, digits = 1)
    
    # Retention
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start        DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End          DATE = @Yesterday;
      
      -- Previous month window for MTD (1st of prev month .. same number of days as current MTD, capped at prev EOM)
      DECLARE @PrevMonthStart   DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End      DATE = CASE
                                         WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                              THEN EOMONTH(@PrevMonthStart)
                                         ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                       END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          p.Period,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
              COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
      ) AS Curr
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
      ) AS Prev
        "
    
    df_ret <- dbGetQuery(conn, query)
    
    # Add Market
    df_ret$Market <-"ALL"
    
    # By market
    
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start      DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End        DATE = @Yesterday;
      
      -- Previous month window for MTD (same number of days, capped to prior EOM)
      DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End    DATE = CASE
                                       WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                            THEN EOMONTH(@PrevMonthStart)
                                       ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                     END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          Curr.Market AS Market,
          p.Period,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              us.COUNTRY AS Market,
              SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP,
              SUM(CASE WHEN b.FTD = 1 THEN 1 ELSE 0 END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
            AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
          GROUP BY us.COUNTRY
      ) AS Curr
      OUTER APPLY (
          SELECT
              SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
            AND us.COUNTRY = Curr.Market
      ) AS Prev
        "
    
    df_ret_markets <- dbGetQuery(conn, query)

    # Others
    
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start        DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End          DATE = @Yesterday;
      
      -- Previous month window for MTD (1st of prev month .. same number of days as current MTD, capped at prev EOM)
      DECLARE @PrevMonthStart   DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End      DATE = CASE
                                         WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                              THEN EOMONTH(@PrevMonthStart)
                                         ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                       END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          p.Period,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
              COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
            AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      ) AS Curr
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
            AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      ) AS Prev;
        "
    
    df_ret_others <- dbGetQuery(conn, query)
    df_ret_others$Market <-"Others"
    
    # Brands
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start        DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End          DATE = @Yesterday;
      
      -- Previous month window for MTD (1st of prev month .. same number of days as current MTD, capped at prev EOM)
      DECLARE @PrevMonthStart   DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End      DATE = CASE
                                         WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                              THEN EOMONTH(@PrevMonthStart)
                                         ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                       END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          p.Period,
          Curr.Market,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              m.Market,
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
              COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          CROSS APPLY (
              VALUES (
                  CASE
                      WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                      WHEN us.BRANDID = 6                           THEN 'BET'
                      ELSE NULL
                  END
              )
          ) AS m(Market)
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
            AND m.Market IS NOT NULL
          GROUP BY m.Market
      ) AS Curr
      OUTER APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          CROSS APPLY (
              VALUES (
                  CASE
                      WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                      WHEN us.BRANDID = 6                           THEN 'BET'
                      ELSE NULL
                  END
              )
          ) AS m(Market)
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
            AND m.Market = Curr.Market
      ) AS Prev;
    "
    
    df_ret_brands <- dbGetQuery(conn, query)
    
        
    # Add all together
    df_main <- rbind(df_main, df_main_markets, df_main_others, df_main_brands)
    df_ret <- rbind(df_ret, df_ret_markets, df_ret_others, df_ret_brands)
    
    df_main <- df_main %>%
      pivot_longer(
        cols = c(DEPs, STAKE, GGR, NGR, Margin),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Period, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")

    df_ret <- df_ret %>%
      pivot_longer(
        cols = c(RMPs, FTDs, Retention),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Period, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    df_main_final <- rbind(df_main, df_ret)
    
    # Total Monthly global values (excluding ALL)
    monthly_shares <- df_main_final %>%
      filter(Period == "30 days", METRIC %in% c("DEPs", "NGR"), Market != "ALL") %>%
      group_by(METRIC) %>%
      mutate(
        global_monthly_total = sum(VALUE, na.rm = TRUE),
        share = VALUE / global_monthly_total
      ) %>%
      ungroup() %>%
      select(Market, METRIC, share)

    
    # days 1..day(yesterday) of the month
    X <- as.integer(yesterday - mtd_start) + 1L
    
    df_main_final <- df_main_final %>%
      left_join(monthly_shares, by = c("Market","METRIC")) %>%
      mutate(
        Target = case_when(
          # --- DEPs: ALL
          METRIC == "DEPs" & Market == "ALL" & Period == "30 days"   ~ 4000000,
          METRIC == "DEPs" & Market == "ALL" & Period == "7 days"    ~ 4000000 * 7/30,
          METRIC == "DEPs" & Market == "ALL" & Period == "Yesterday" ~ 4000000 / 30,
          METRIC == "DEPs" & Market == "ALL" & Period == "MTD"       ~ 4000000 * X/30,
          
          # --- DEPs: other markets
          METRIC == "DEPs" & Market != "ALL" & Period == "30 days"   ~ share * 4000000,
          METRIC == "DEPs" & Market != "ALL" & Period == "7 days"    ~ share * 4000000 * 7/30,
          METRIC == "DEPs" & Market != "ALL" & Period == "Yesterday" ~ share * 4000000 / 30,
          METRIC == "DEPs" & Market != "ALL" & Period == "MTD"       ~ share * 4000000 * X/30,
          
          # --- NGR: ALL
          METRIC == "NGR" & Market == "ALL" & Period == "30 days"    ~ 2600000,
          METRIC == "NGR" & Market == "ALL" & Period == "7 days"     ~ 2600000 * 7/30,
          METRIC == "NGR" & Market == "ALL" & Period == "Yesterday"  ~ 2600000 / 30,
          METRIC == "NGR" & Market == "ALL" & Period == "MTD"        ~ 2600000 * X/30,
          
          # --- NGR: other markets
          METRIC == "NGR" & Market != "ALL" & Period == "30 days"    ~ share * 2600000,
          METRIC == "NGR" & Market != "ALL" & Period == "7 days"     ~ share * 2600000 * 7/30,
          METRIC == "NGR" & Market != "ALL" & Period == "Yesterday"  ~ share * 2600000 / 30,
          METRIC == "NGR" & Market != "ALL" & Period == "MTD"        ~ share * 2600000 * X/30,
          
          # --- Retention ---
          METRIC == "Retention" ~ 80,
          
          # --- Margin ---
          METRIC == "Margin" ~ 5,
          
          # --- Other metrics ---
          TRUE ~ 0
        ),
        DIFF_ABSOLUTE   = VALUE - Target,
        DIFF_PERCENTAGE = ifelse(Target == 0, NA, round((VALUE - Target) / Target * 100, 2))
      )
    df_main_final$share = NULL

    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0("aiPerformanceFiles/", Sys.Date(), "_ai-summary-targets_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    #####################################################################
    ######################### Main metrics Graph ########################
    #####################################################################    
    
    query <- "

    SELECT
        b.[Date],

        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE
        b.[Date] >= '2024-10-01'
        
    GROUP BY
        b.[Date]
    "
    df <- dbGetQuery(conn, query) 
    
    # Add Market and margin
    df$Margin <- round((df$GGR / df$STAKE) * 100, digits = 1)
    df$Market <-"ALL"
    
    
    query <- "

    SELECT
        b.[Date],
        us.COUNTRY AS Market,
    
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b

    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= '2024-10-01' 
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
        
    GROUP BY
        b.[Date],
        us.COUNTRY
    "
    df_markets <- dbGetQuery(conn, query)
    
    # Add margin
    df_markets$Margin <- round((df_markets$GGR / df_markets$STAKE) * 100, digits = 1)
    
    query <- "

    SELECT
        b.[Date],
        SUM(b.DEPOSIT_AMOUNT) AS DEPs,
        SUM(b.Total_STAKE)    AS STAKE,
        SUM(b.Total_GGR)      AS GGR,
        SUM(b.Total_NGR)      AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users AS us
      ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= '2024-10-01'
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
    GROUP BY
        b.[Date];
    "
    df_others <- dbGetQuery(conn, query) 
    
    df_others$Margin <- round((df_others$GGR / df_others$STAKE) * 100, digits = 1)
    df_others$Market <-"Others"
    
    query <- "

    SELECT
        b.[Date],
        m.Market,
        SUM(b.DEPOSIT_AMOUNT) AS DEPs,
        SUM(b.Total_STAKE)    AS STAKE,
        SUM(b.Total_GGR)      AS GGR,
        SUM(b.Total_NGR)      AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users AS us
      ON us.GL_ACCOUNT = b.PARTYID
    CROSS APPLY (
        VALUES (
            CASE
                WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                WHEN us.BRANDID = 6                           THEN 'BET'
                ELSE NULL
            END
        )
    ) AS m(Market)
    WHERE
        b.[Date] >= '2024-10-01'
        AND m.Market IS NOT NULL
    GROUP BY
        b.[Date],
        m.Market;
    "
    df_brands <- dbGetQuery(conn, query)
    
    # Add margin
    df_brands$Margin <- round((df_brands$GGR / df_brands$STAKE) * 100, digits = 1) 
    
    # Add all together
    df <- rbind(df, df_markets, df_others, df_brands)

    df <- df %>%
      pivot_longer(
        cols = c(DEPs, STAKE, GGR, NGR, Margin, RMPs, FTDs),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs")) %>%   # exclude Margin, will recalc
      group_by(Week, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Week, Market), names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Week)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs")) %>%   # exclude Margin, will recalc
      group_by(Month, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Month, Market), names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    # --- MTD ---
    df_MTD <- df %>%
      mutate(Date = as.Date(Date)) %>%
      # keep same metrics you aggregate elsewhere (Margin will be recomputed)
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs")) %>%
      # keep only days 1..cutoff_day (day(yesterday)) for every month in the series
      filter(day(Date) <= day(yesterday)) %>%
      # bucket by month
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      # recompute Margin; avoid divide-by-zero
      mutate(Margin = round(if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_), 1)) %>%
      pivot_longer(-c(Month, Market), names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-main-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-main-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-main-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-main-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
 
    #####################################################################
    ########################## Main differences #########################
    #####################################################################
    
    agg_period <- function(df, start_date, end_date) {
      df %>%
        filter(Date >= start_date, Date <= end_date, METRIC %in% metrics_base) %>%
        group_by(Market, METRIC) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
        pivot_wider(names_from = METRIC, values_from = VALUE) %>%
        mutate(Margin = if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_)) %>%
        pivot_longer(-Market, names_to = "METRIC", values_to = "VALUE")
    }
    
    windows_for_period <- function(yesterday, period) {
      end_cur <- as.Date(yesterday)
      if (period == "Yesterday") {
        start_cur <- end_cur
        start_prev <- end_cur - 1L
        end_prev <- start_prev
      } else if (period == "7 days") {
        start_cur <- end_cur - 6L
        end_prev <- start_cur - 1L
        start_prev <- end_prev - 6L
      } else if (period == "30 days") {
        start_cur <- end_cur - 29L
        end_prev <- start_cur - 1L
        start_prev <- end_prev - 29L
      } else if (period == "MTD") {
        start_cur <- floor_date(end_cur, "month")
        X <- as.integer(end_cur - start_cur) + 1L
        start_prev <- start_cur %m-% months(1)
        prev_month_end <- (start_prev %m+% months(1)) - days(1)
        end_prev <- min(start_prev + days(X - 1L), prev_month_end)
      } else stop("Invalid period")
      list(start_cur = start_cur, end_cur = end_cur,
           start_prev = start_prev, end_prev = end_prev)
    }
    
    build_comp_for_period <- function(df, yesterday, period) {
      w <- windows_for_period(yesterday, period)
      cur <- agg_period(df, w$start_cur, w$end_cur) %>% mutate(Period = period)
      prev <- agg_period(df, w$start_prev, w$end_prev) %>%
        mutate(Period = period) %>% rename(Previous = VALUE)
      cur %>%
        left_join(prev, by = c("Market","METRIC","Period")) %>%
        mutate(
          DIFF_ABSOLUTE   = VALUE - Previous,
          DIFF_PERCENTAGE = if_else(is.na(Previous) | Previous == 0, NA_real_,
                                    round((VALUE - Previous)/Previous * 100, 2))
        )
    }
    
    df_comp <- bind_rows(
      build_comp_for_period(df, yesterday, "Yesterday"),
      build_comp_for_period(df, yesterday, "7 days"),
      build_comp_for_period(df, yesterday, "30 days"),
      build_comp_for_period(df, yesterday, "MTD")
    )
    
    
    # Retention
    # Build daily RMP/FTD (include 'ALL' aggregate so it matches df_ret)
    daily_raw <- df %>%
      filter(METRIC %in% c("RMPs","FTDs")) %>%
      group_by(Date, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE, values_fill = 0) %>%
      rename(RMP = RMPs, FTD = FTDs)
    
    daily_all <- daily_raw %>%
      group_by(Date) %>%
      summarise(RMP = sum(RMP, na.rm = TRUE),
                FTD = sum(FTD, na.rm = TRUE), .groups = "drop") %>%
      mutate(Market = "ALL")
    
    daily <- bind_rows(daily_raw, daily_all)
    
    eom <- function(d) floor_date(d, "month") %m+% months(1) - days(1)
    
    windows_for_period <- function(y, period) {
      y <- as.Date(y)
      if (period == "Yesterday") {
        list(cur=c(y,y), prev=c(y-1,y-1), pprev=c(y-2,y-2))
      } else if (period == "7 days") {
        cur_s <- y-6; prev_e <- cur_s-1; prev_s <- prev_e-6
        pprev_e <- prev_s-1; pprev_s <- pprev_e-6
        list(cur=c(cur_s,y), prev=c(prev_s,prev_e), pprev=c(pprev_s,pprev_e))
      } else if (period == "30 days") {
        cur_s <- y-29; prev_e <- cur_s-1; prev_s <- prev_e-29
        pprev_e <- prev_s-1; pprev_s <- pprev_e-29
        list(cur=c(cur_s,y), prev=c(prev_s,prev_e), pprev=c(pprev_s,pprev_e))
      } else if (period == "MTD") {
        cur_s <- floor_date(y, "month"); X <- as.integer(y - cur_s) + 1L
        prev_s <- cur_s %m-% months(1); prev_e <- pmin(prev_s + days(X-1L), eom(prev_s))
        pprev_s <- prev_s %m-% months(1); pprev_e <- pmin(pprev_s + days(X-1L), eom(pprev_s))
        list(cur=c(cur_s,y), prev=c(prev_s,prev_e), pprev=c(pprev_s,pprev_e))
      } else stop("Invalid period")
    }
    
    sum_window <- function(win) {
      daily %>%
        filter(Date >= win[1], Date <= win[2]) %>%
        group_by(Market) %>%
        summarise(RMP = sum(RMP, na.rm = TRUE),
                  FTD = sum(FTD, na.rm = TRUE), .groups = "drop")
    }
    
    build_prev_retention <- function(period) {
      w <- windows_for_period(yesterday, period)
      prev   <- sum_window(w$prev)
      pprev  <- sum_window(w$pprev) %>% rename(RMP_pprev = RMP) %>% select(Market, RMP_pprev)
      prev %>%
        left_join(pprev, by = "Market") %>%
        transmute(Market, Period = period,
                  Previous = round(if_else(RMP_pprev > 0, (RMP - FTD)/RMP_pprev * 100, NA_real_), 2))
    }
    
    prev_long <- bind_rows(
      build_prev_retention("Yesterday"),
      build_prev_retention("7 days"),
      build_prev_retention("30 days"),
      build_prev_retention("MTD")
    )
    
    df_ret_final <- df_ret %>%
      filter(METRIC == "Retention") %>%            # keep long structure
      left_join(prev_long, by = c("Market","Period")) %>%
      mutate(
        DIFF_ABSOLUTE   = VALUE - Previous,
        DIFF_PERCENTAGE = if_else(is.na(Previous) | Previous == 0,
                                  NA_real_,
                                  round((VALUE - Previous)/Previous * 100, 2))
      )
    
    #Join
    df_main_final <- rbind(df_comp, df_ret_final)
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0("aiPerformanceFiles/", Sys.Date(), "_ai-summary-differences_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    #####################################################################
    ######################## Deposit Group Graph ########################
    #####################################################################    
    
    query <- "

    SELECT
        b.[Date],
        
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END AS FTD_Group,
        
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount) AS NET,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN (SELECT PARTYID, [Date], FTD_Since_Months FROM bi_data.dbo.RetentionUsers WHERE [Date] >= '2024-10-01') AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
    WHERE
        b.[Date] >= '2024-10-01'
        AND u.FTD_Since_Months >= 0
        
    GROUP BY
        b.[Date],
        u.FTD_Since_Months,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END

    "
    df <- dbGetQuery(conn, query) 
    df$Margin <- round((df$GGR / df$STAKE) * 100, digits = 1)
    df$Market <-"ALL"
    
    
    query <- "
        
        SELECT
        b.[Date],
        us.COUNTRY AS Market,
        
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END AS FTD_Group,
        
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount) AS NET,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    INNER JOIN (SELECT PARTYID, [Date], FTD_Since_Months FROM bi_data.dbo.RetentionUsers WHERE [Date] >= '2024-10-01') AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
    WHERE
        b.[Date] >= '2024-10-01'
        AND u.FTD_Since_Months >= 0
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date],
        us.COUNTRY,
        u.FTD_Since_Months,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END
    "
    df_markets <- dbGetQuery(conn, query)
    df_markets$Margin <- round((df_markets$GGR / df_markets$STAKE) * 100, digits = 1)
 
    
    query <- "

      SELECT
          b.[Date],
          g.FTD_Group,
          SUM(b.DEPOSIT_AMOUNT)                                        AS DEPs,
          SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount)             AS NET,
          SUM(b.Total_STAKE)                                           AS STAKE,
          SUM(b.Total_GGR)                                             AS GGR,
          SUM(b.Total_NGR)                                             AS NGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END)       AS RMPs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN (
          SELECT PARTYID, [Date], FTD_Since_Months
          FROM bi_data.dbo.RetentionUsers
          WHERE [Date] >= '2024-10-01'
      ) AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE 
                WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
                WHEN u.FTD_Since_Months BETWEEN 1  AND 3  THEN '[1-3]'
                WHEN u.FTD_Since_Months BETWEEN 4  AND 12 THEN '[4-12]'
                WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
                WHEN u.FTD_Since_Months > 24              THEN '[> 25]'
                ELSE NULL
              END
          )
      ) AS g(FTD_Group)
      WHERE
          b.[Date] >= '2024-10-01'
          AND u.FTD_Since_Months >= 0
          AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
          AND g.FTD_Group IS NOT NULL
      GROUP BY
          b.[Date],
          g.FTD_Group;

    "
    df_others <- dbGetQuery(conn, query) 
    df_others$Margin <- round((df_others$GGR / df_others$STAKE) * 100, digits = 1)
    df_others$Market <-"Others"
    
    
    query <- "
      SELECT
          b.[Date],
          m.Market,
          g.FTD_Group,
          SUM(b.DEPOSIT_AMOUNT)                                        AS DEPs,
          SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount)             AS NET,
          SUM(b.Total_STAKE)                                           AS STAKE,
          SUM(b.Total_GGR)                                             AS GGR,
          SUM(b.Total_NGR)                                             AS NGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END)       AS RMPs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN (
          SELECT PARTYID, [Date], FTD_Since_Months
          FROM bi_data.dbo.RetentionUsers
          WHERE [Date] >= '2024-10-01'
      ) AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      CROSS APPLY (
          VALUES (
              CASE 
                WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
                WHEN u.FTD_Since_Months BETWEEN 1  AND 3  THEN '[1-3]'
                WHEN u.FTD_Since_Months BETWEEN 4  AND 12 THEN '[4-12]'
                WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
                WHEN u.FTD_Since_Months > 24              THEN '[> 25]'
                ELSE NULL
              END
          )
      ) AS g(FTD_Group)
      WHERE
          b.[Date] >= '2024-10-01'
          AND u.FTD_Since_Months >= 0
          AND m.Market IS NOT NULL
          AND g.FTD_Group IS NOT NULL
      GROUP BY
          b.[Date],
          m.Market,
          g.FTD_Group;

    "
    df_brands <- dbGetQuery(conn, query)
    df_brands$Margin <- round((df_brands$GGR / df_brands$STAKE) * 100, digits = 1)
    
    #All together
    df <- rbind(df, df_markets, df_others, df_brands)
    
    df <- df %>%
      pivot_longer(
        cols = c(DEPs, NET, STAKE, GGR, NGR, Margin, RMPs),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      # exclude Margin, recalc later
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs")) %>% 
      group_by(Week, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Week, Market, FTD_Group),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Week)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs")) %>% 
      group_by(Month, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Month, Market, FTD_Group),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    # --- MTD ---
    df_MTD <- df %>%
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs"),
             day(Date) <= day(yesterday)) %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round(if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_), 1)) %>%
      pivot_longer(-c(Month, Market, FTD_Group),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
    
    #####################################################################
    ##################### Deposit group main metrics ####################
    #####################################################################
    
    # Ensure Date is a proper Date type
    df$Date <- as.Date(df$Date)
    
    # Define "yesterday" as the max date in df
    yesterday <- max(df$Date, na.rm = TRUE)
    
    # --- MTD helpers (current month through yesterday vs same days last month) ---
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    # Number of days in the current MTD window (0-based offset from the 1st)
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    # End of previous month
    prev_eom <- seq(prev_month_start, by = "1 month", length.out = 2)[2] - 1
    # Previous MTD end = same offset days in prev month, capped at prev EOM
    prev_mtd_end <- min(prev_month_start + mtd_offset_days, prev_eom)
    
    # Period windows
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    # Keep only metrics of interest
    df_filtered <- df %>% filter(METRIC %in% c("DEPs", "RMPs", "NGR"))
    
    # Function to calculate sums and compare with previous period
    calc_period <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC, FTD_Group) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC, FTD_Group) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      # Join and calculate diffs
      full_join(current, previous, by = c("Market", "METRIC", "FTD_Group")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(PREVIOUS == 0, NA_real_,
                                   round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, FTD_Group, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    # Apply to all periods and bind results
    df_main_final <- bind_rows(
      calc_period("Yesterday", periods[["Yesterday"]]),
      calc_period("7 days",    periods[["7 days"]]),
      calc_period("30 days",   periods[["30 days"]]),
      calc_period("MTD",       periods[["MTD"]])
    )
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0("aiPerformanceFiles/", Sys.Date(), "_ai-summary-groups_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    
    #####################################################################
    ######################### Running campaigns #########################
    #####################################################################   
    
    query <- "

    SELECT
        b.[Date],
        d.PNID,
        SUM(ISNULL(b.Total_NGR, 0)) AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
    INNER JOIN (
        SELECT DISTINCT PARTYID, PNID
        FROM bi_data.dbo.MediaCost_NRT
    ) AS m
        ON m.PARTYID = b.PARTYID
    INNER JOIN (
        SELECT *
        FROM bi_data.dbo.MediaDIC_NRT
        WHERE Active = 1
    ) AS d
        ON d.PNID = m.PNID
    GROUP BY
        b.[Date],
        d.PNID

    "
    df_ngr <- dbGetQuery(conn, query) 
    
    query <- "

    SELECT
        CONVERT(date, us.REG_DATE) AS [Date],
        d.PNID,
        d.Country_Omega AS Market,
        d.Device_Omega AS DEVICE,
        SUM(ISNULL(cost.COST, 0)) AS COST
    FROM bi_data.dbo.Users us
    INNER JOIN (
        SELECT DISTINCT PARTYID, PNID
        FROM bi_data.dbo.MediaCost_NRT
    ) AS m
        ON m.PARTYID = us.GL_ACCOUNT
    INNER JOIN (
        SELECT *
        FROM bi_data.dbo.MediaDIC_NRT
        WHERE Active = 1
    ) AS d
        ON d.PNID = m.PNID
    LEFT JOIN (
        SELECT PARTYID, SUM(CPA) AS COST
        FROM bi_data.dbo.Media_CPA
        GROUP BY PARTYID
    ) AS cost
        ON cost.PARTYID = m.PARTYID
    GROUP BY
        CONVERT(date, us.REG_DATE),
        d.PNID,
        d.Country_Omega,
        d.Device_Omega;

    "
    df_cost <- dbGetQuery(conn, query) 
    
    # --- Step 0: drop PNIDs where COST is 0 for all dates ---
    pnids_keep <- df_cost %>%
      group_by(PNID) %>%
      summarise(total_cost = sum(COST, na.rm = TRUE), .groups = "drop") %>%
      filter(total_cost > 0) %>%
      pull(PNID)
    
    df_cost <- df_cost %>% filter(PNID %in% pnids_keep)
    df_ngr <- df_ngr %>% filter(PNID %in% pnids_keep)
    
    # 1) Make sure Date is Date type in both
    df_cost <- df_cost %>% mutate(Date = as.Date(Date))
    df_ngr  <- df_ngr  %>% mutate(Date = as.Date(Date))
    
    
    # 3) Build a PNID→(Market, DEVICE) lookup (one-to-one as you noted)
    pnid_lookup <- df_cost %>%
      distinct(PNID, Market, DEVICE)
    
    # 4) Build the complete dictionary of (Date, PNID)
    dict <- bind_rows(
      df_cost %>% select(Date, PNID),
      df_ngr  %>% select(Date, PNID)
    ) %>% distinct()
    
    # 5) Join measures and fill missing with 0; attach Market/DEVICE via PNID
    df <- dict %>%
      left_join(df_cost %>% select(Date, PNID, COST), by = c("Date", "PNID")) %>%
      left_join(df_ngr,                                   by = c("Date", "PNID")) %>%
      mutate(
        COST = replace_na(COST, 0),
        NGR  = replace_na(NGR,  0)
      ) %>%
      left_join(pnid_lookup, by = "PNID") %>%
      relocate(Market, DEVICE, .after = PNID) %>%
      arrange(Date, PNID)
    
    # Calculate acc ROI
    df <- df %>%
      group_by(PNID, Market, DEVICE) %>%
      arrange(Date, .by_group = TRUE) %>%
      mutate(
        COST_ACC = cumsum(COST),
        NGR_ACC  = cumsum(NGR),
        ROI = ifelse(COST_ACC > 0,
                     round((NGR_ACC / COST_ACC) * 100, 0),
                     NA_real_)
      ) %>%
      ungroup()
    
    # We are not interested in the ACCs anymore
    df <- df %>%
      pivot_longer(
        cols = c("COST", "NGR", "ROI" ),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      filter(METRIC %in% c("COST", "NGR")) %>%
      group_by(Week, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      rename(Date = Week)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("COST", "NGR")) %>%
      group_by(Month, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      rename(Date = Month)
    
    # --- MTD ---
    df_MTD <- df %>%
      filter(METRIC %in% c("COST","NGR"), day(Date) <= day(yesterday)) %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      rename(Date = Month)
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-running-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df_weekly[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-running-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-running-monthly-", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-running-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
    }

    
    
    #####################################################################
    ########################## Running metrics ##########################
    ##################################################################### 
    
    # Ensure Date is a proper Date type
    df$Date <- as.Date(df$Date)
    
    # Define yesterday (max date in your df)
    yesterday <- max(df$Date, na.rm = TRUE)
    
    # --- MTD helpers (1st of current month -> yesterday vs same #days last month, capped at prev EOM)
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    prev_eom <- seq(prev_month_start, by = "1 month", length.out = 2)[2] - 1
    prev_mtd_end <- min(prev_month_start + mtd_offset_days, prev_eom)
    
    # Period windows
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    # Keep only ROI
    df_filtered <- df %>% filter(METRIC == "ROI")
    
    # Function to calculate sums and compare with previous period
    calc_period <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC, PNID, DEVICE) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC, PNID, DEVICE) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      # Join and calculate diffs
      full_join(current, previous, by = c("Market", "METRIC", "PNID", "DEVICE")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE  = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                   NA_real_,
                                   round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, PNID, DEVICE,
               VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    # Apply to all periods and bind results
    df_main_final <- bind_rows(
      calc_period("Yesterday", periods[["Yesterday"]]),
      calc_period("7 days",    periods[["7 days"]]),
      calc_period("30 days",   periods[["30 days"]]),
      calc_period("MTD",       periods[["MTD"]])
    )
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0("aiPerformanceFiles/", Sys.Date(), "_ai-summary-running_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    #####################################################################
    ########################### Product Graph ###########################
    #####################################################################  
    
    query <- "

    SELECT
        b.[Date],
        
        b.GAME_CATEGORY,
        
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE
        b.[Date] >= '2024-10-01'

    GROUP BY
        b.[Date],
        b.GAME_CATEGORY

    "
    df <- dbGetQuery(conn, query) 
    df$Margin <- round((df$GGR / df$STAKE) * 100, digits = 1)
    df$Market <-"ALL"
    
    query <- "
        
        SELECT
        b.[Date],
        us.COUNTRY AS Market,
        
        b.GAME_CATEGORY,
        
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= '2024-10-01'
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date],
        us.COUNTRY,
        b.GAME_CATEGORY
    "
    df_markets <- dbGetQuery(conn, query)
    df_markets$Margin <- round((df_markets$GGR / df_markets$STAKE) * 100, digits = 1)

    query <- "
        
      SELECT
          b.[Date],
          b.GAME_CATEGORY,
          SUM(b.Total_STAKE)                                     AS STAKE,
          SUM(b.Total_GGR)                                       AS GGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE
          b.[Date] >= '2024-10-01'
          AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      GROUP BY
          b.[Date],
          b.GAME_CATEGORY;
    "
    df_others <- dbGetQuery(conn, query)
    df_others$Margin <- round((df_others$GGR / df_others$STAKE) * 100, digits = 1)
    df_others$Market <-"Others"
    
    query <- "

      SELECT
          b.[Date],
          b.GAME_CATEGORY,
          m.Market,
          SUM(b.Total_STAKE)                                     AS STAKE,
          SUM(b.Total_GGR)                                       AS GGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE
          b.[Date] >= '2024-10-01'
          AND m.Market IS NOT NULL
      GROUP BY
          b.[Date],
          b.GAME_CATEGORY,
          m.Market;

    "
    df_brands <- dbGetQuery(conn, query) 
    df_brands$Margin <- round((df_brands$GGR / df_brands$STAKE) * 100, digits = 1)
    
    
    # All together
    df <- rbind(df, df_markets, df_others, df_brands)
    
    # Filter out some categories
    df <- df %>%
      filter(
        !is.na(GAME_CATEGORY),
        !GAME_CATEGORY %in% c("Loyalty", "CASHBACK", "OTHER MAN BONUS")
      )
    
    
    df <- df %>%
      pivot_longer(
        cols = c(STAKE, GGR, Margin, RMPs),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      # exclude Margin, recalc later
      filter(METRIC %in% c("GGR","STAKE","RMPs")) %>% 
      group_by(Week, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Week, Market, GAME_CATEGORY),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Week)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("GGR","STAKE","RMPs")) %>% 
      group_by(Month, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Month, Market, GAME_CATEGORY),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    # --- MTD ---
    df_MTD <- df %>%
      filter(METRIC %in% c("GGR","STAKE","RMPs"), day(Date) <= day(yesterday)) %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round(if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_), 1)) %>%
      pivot_longer(-c(Month, Market, GAME_CATEGORY),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-product-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-product-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-product-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-product-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
    }

    #####################################################################
    ######################## Product main metrics #######################
    #####################################################################
    
    # Ensure Date is a proper Date type
    df$Date <- as.Date(df$Date)
    
    # Define yesterday (max date in your df)
    yesterday <- max(df$Date, na.rm = TRUE)
    
    # --- MTD helpers (1st of current month -> yesterday vs same #days last month, capped at prev EOM)
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    prev_eom <- seq(prev_month_start, by = "1 month", length.out = 2)[2] - 1
    prev_mtd_end <- min(prev_month_start + mtd_offset_days, prev_eom)
    
    # Period windows
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    # Keep only metrics of interest
    df_filtered <- df %>% filter(METRIC %in% c("GGR", "RMPs"))
    
    # Function to calculate sums and compare with previous period
    calc_period <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC, GAME_CATEGORY) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC, GAME_CATEGORY) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      full_join(current, previous, by = c("Market", "METRIC", "GAME_CATEGORY")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE  = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                   NA_real_,
                                   round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, GAME_CATEGORY, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    # Apply to all periods and bind results
    df_main_final <- bind_rows(
      calc_period("Yesterday", periods[["Yesterday"]]),
      calc_period("7 days",    periods[["7 days"]]),
      calc_period("30 days",   periods[["30 days"]]),
      calc_period("MTD",       periods[["MTD"]])
    )
    
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0("aiPerformanceFiles/", Sys.Date(), "_ai-summary-product_", mkt, ".csv"),
        row.names = FALSE
      )        
    }  
    
    #####################################################################
    ########################## Retention Graph ##########################
    #####################################################################
    
    query <- "

    SELECT
        FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
        
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs,
        COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
        COUNT(DISTINCT CASE WHEN b.RETAINED = 1 THEN b.PARTYID END) AS RETAINED
    
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE b.[Date] >= '2025-01-01'
    GROUP BY FORMAT(b.[Date], 'yyyy-MM')

    "
    df <- dbGetQuery(conn, query) 
    df$Market <-"ALL"
    
    query <- "
        
    SELECT
        FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
        us.COUNTRY AS Market,
        
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs,
        COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
        COUNT(DISTINCT CASE WHEN b.RETAINED = 1 THEN b.PARTYID END) AS RETAINED
    
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    WHERE 
      b.[Date] >= '2025-01-01'
      AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY 
      FORMAT(b.[Date], 'yyyy-MM'), 
      us.COUNTRY
    
    "
    df_markets <- dbGetQuery(conn, query)
    
    query <- "

      SELECT
          FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
          COUNT(DISTINCT CASE WHEN b.FTD = 1        THEN b.PARTYID END) AS FTDs,
          COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
          COUNT(DISTINCT CASE WHEN b.RETAINED = 1    THEN b.PARTYID END) AS RETAINED
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE
          b.[Date] >= '2025-01-01'
          AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      GROUP BY
          FORMAT(b.[Date], 'yyyy-MM');

    "
    df_others <- dbGetQuery(conn, query) 
    df_others$Market <-"ALL"
    
    query <- "
        
      SELECT
          FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
          m.Market,
          COUNT(DISTINCT CASE WHEN b.FTD = 1        THEN b.PARTYID END) AS FTDs,
          COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
          COUNT(DISTINCT CASE WHEN b.RETAINED = 1    THEN b.PARTYID END) AS RETAINED
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE
          b.[Date] >= '2025-01-01'
          AND m.Market IS NOT NULL
      GROUP BY
          FORMAT(b.[Date], 'yyyy-MM'),
          m.Market;
    
    "
    df_brands <- dbGetQuery(conn, query)
    
    df <- rbind(df, df_markets, df_others, df_brands)
    
    df$Date <- as.Date(paste0(df$YearMonth, "-01"), format = "%Y-%m-%d")
    
    df <- df %>%
      pivot_longer(
        cols = c("FTDs", "RETAINED", "REACTIVATED" ),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        "aiPerformanceFiles/", Sys.Date(),
        "_ai-retention-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
    
    #####################################################################
    ######################### Retention metrics #########################
    #####################################################################
    
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # Keep only relevant metrics
    df_filtered <- df %>% 
      filter(METRIC %in% c("FTDs", "RETAINED", "REACTIVATED"))
    
    # ---------------------------
    # Window-based (Yesterday / 7 days / 30 days / MTD)
    # ---------------------------
    yesterday <- max(df_filtered$Date, na.rm = TRUE)
    
    # MTD helpers
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    prev_eom <- (prev_month_start %m+% months(1)) - days(1)
    prev_mtd_end <- pmin(prev_month_start + mtd_offset_days, prev_eom)
    
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    calc_window <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      full_join(current, previous, by = c("Market", "METRIC")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE  = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                   NA_real_, round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    df_windows <- bind_rows(
      calc_window("Yesterday", periods[["Yesterday"]]),
      calc_window("7 days",    periods[["7 days"]]),
      calc_window("30 days",   periods[["30 days"]]),
      calc_window("MTD",       periods[["MTD"]])
    )
    
    # ---------------------------
    # Monthly comparisons (MoM and 3MoM)
    # ---------------------------
    # Monthly aggregation
    df_monthly <- df_filtered %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Market, METRIC, Month) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    current_month <- floor_date(yesterday, "month")
    prev_month    <- current_month %m-% months(1)
    month_minus3  <- current_month %m-% months(3)
    
    # MoM: current month vs previous month
    mom_curr <- df_monthly %>% filter(Month == current_month) %>% select(-Month)
    mom_prev <- df_monthly %>% filter(Month == prev_month)    %>% select(-Month) %>%
      rename(PREVIOUS = VALUE)
    
    df_mom <- full_join(mom_curr, mom_prev, by = c("Market","METRIC")) %>%
      mutate(
        Period = "MoM",
        DIFF_ABSOLUTE  = VALUE - PREVIOUS,
        DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                 NA_real_, round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
      ) %>%
      select(Period, Market, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    
    # 3MoM: current month vs the month 3 months ago
    mom3_curr <- df_monthly %>% filter(Month == current_month) %>% select(-Month)
    mom3_prev <- df_monthly %>% filter(Month == month_minus3)  %>% select(-Month) %>%
      rename(PREVIOUS = VALUE)
    
    df_3mom <- full_join(mom3_curr, mom3_prev, by = c("Market","METRIC")) %>%
      mutate(
        Period = "3MoM",
        DIFF_ABSOLUTE  = VALUE - PREVIOUS,
        DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                 NA_real_, round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
      ) %>%
      select(Period, Market, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    
    # ---------------------------
    # Final output
    # ---------------------------
    df_main_final <- bind_rows(
      df_windows,
      df_mom,
      df_3mom
    ) 
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0("aiPerformanceFiles/", Sys.Date(), "_ai-summary-retention_", mkt, ".csv"),
        row.names = FALSE
      )          
    }
    
    
  }, error = function(e) {
    retry_attempts <<- retry_attempts + 1
    write_log(paste("Connection failed. Retry attempt:", retry_attempts, "of", max_retries, "Error:", e$message))
    
    if (retry_attempts < max_retries) {
      Sys.sleep(retry_interval)  # Wait before retrying
    } else {
      write_log(paste("The database is locked or faces a related problem. No action was take:", e$message), level = "ERROR")
      write_log("Maximum retry attempts reached. Unable to connect to the database.", level = "ERROR")
      stop("Maximum retry attempts reached. Exiting script.")
    }
  })
}

write_log(paste0("Finishing at ",Sys.time()))
dbDisconnect(conn)
gc()


