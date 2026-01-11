## -------------------------------------------------------------
## -------------------------------------------------------------
## This script is to transfer maps of EP to EP-named directories
## so that it checks the existence of the directories and creates
## them if they don't exist
## -------------------------------------------------------------
## -------------------------------------------------------------

# Project names
EPname <- c("AR-RAHMANIYAH", "UTAYQAH", "AL-FUTAH", "AL-MUANISIYAH", "AL-KHUZAMA", "AD-DIRAH", "SALAM", "Al Shohlah", "AL-MANAKH", "BINBAN", 
            "AL-MARGAB", "AL-MALAZ", "AL-MASANI", "ORAID", "Al Rayah", "SHUBRA", "AS-SINAIYAH", "KING KHALID INT.AIRPORT", "AL-FAKHIRIYAH", 
            "AD-DIFAA", "OHOD", "MANFUHA AL-JADEEDAH", "Al Ola", "AL-URAIJA", "EAST AN-NASEEM", "AL-KHALIDIAH", "No Name", "EAST UMM AL-HAMAM", 
            "AMAH", "AZ-ZAHRAA", "AD-DOBBAT", "SIYAH", "AL-MUROOJ", "AL-JANADRIYAH", "KHASHM AL ANN", "AN-NAHDAH", "SULTANAH", "AL-QURA", 
            "KING SAUD UNIVERSITY", "AL-JARRADIYAH", "AD-DAHOU", "AL-WOROUD", "QURTUBAH", "WEST AN-NASEEM", "AL-AZIZIYAH", "AS-SAFARAT", 
            "AL-HAIER", "AL-MASHAEL", "AL- NADWAH", "AR-RABI", "AL-AOUD", "KING FAHD", "ASH-SHOHDA", "OKAZ", "AN-NADHEEM", "AL-JAZEERAH", 
            "KING ABDULLAH CITY ENERGY", "AD-DUBIYAH", "AL-GHADEER", "AL-MASEEF", "AL-BASATEEN", "AT-TAAWUN", "AS-SAFA", "AL-MANAR", "No Name", 
            "AL-RABIA", "AL-MATHAR", "Al Tadamon", "HEET", "AL-GHANAMIYAH", "AL-ANDALUS", "AS-SULAI", "AL-MALQA", "WADY LABAN", "AL-BOTAIHA", 
            "AL-EZDIHAR", "AS-SULAIMANYAH", "Al Zahour", "AL-NUZHA", "NORTH AL-MATHAR", "NEW INDUSTRIAL CITY", "AR-RAED", "ASH-SHARAFIYAH", 
            "AL-MAHDIYAH", "AS-SUWAIDI AL-GHARBI", "AS-SALAM", "GHUBAIRA", "AL-WISHAM", "Al Majad", "SKIRINAH", "AN-NADA", "AL-WESITA", 
            "AD-DURAIHEMIYAH", "MUGHARAZAT", "AN-NARJIS", "Al Danah", "No Name", "AR-RAWDAH", "AR-RAWABI", "Al Risalah", "AL-WADI", "ASH-SHOMAISI", 
            "AS SIDRA", "AN-NAMUTHAJIYAH", "AL-MAIZALIYAH", "SALAH AD DEEN", "AL-AMAJIYAH", "AL-MARWAH", "AN-NOOR", "AL-ARID", "AD-DAR AL- BAIDA", 
            "AL-MURABA", "THULAIM", "AL-KHALEEJ", "AL-HAMRA", "AL-FALAH", "AL-QADISIYAH", "AS-SAHAFA", "ASH-SHIFA", "AS-SALHIYAH", "AL-MANSURAH", 
            "Al Fursan", "AL-HADA", "AL-NAKHEEL", "AL-QUDS", "TAYBAH", "AL-FARUQ", "AL-KHEAR", "AL-MISFAH", "KING ABDUL AZIZ", "AL-FAIHA", 
            "AL-MUTAMARAT", "AR-RABWAH", "DAHIAT NAMAR", "WEST AL-ORAIJA", "KING ABDULLAH", "AL-Awaly", "AL-MUHAMMADIYAH", "HITTEEN", "AL-YASMEEN", 
            "Al Rihab", "DAHRAT AL-BADEAH", "IRQAH", "AL-QAIRAWAN", "AS-SUWAIDI", "AL-AMAL", "AL-BADEAH", "Al Nakhbah", "AL-MURSALAT", "JAREER", 
            "MEEKAL", "AL-WAHAH", "AL-FAISALIYAH", "AL-AQEEK", "AZ-ZAHRA", "GHIRNATAH", "MANFUHA", "AR-RAYAN", "DAHRAT-AL-ODAH-GHARB-AD DIRIYAH", 
            "AL-MALQA-AD DIRIYAH", "AL-ISKAN", "KING FAISAL", "AL-YARMUK", "AN-NASIRIYAH", "Al Zaher", "AR-RIMAL", "AL-BARIYAH", "NAMAR", "UMM-SALEEM", 
            "AN-NAFEL", "AL-WIZARAT", "AL-MANSURIAH", "AL-HAZM", "AL-OLAYA", "AL-ASEMAH-AD DIRIYAH", "AL-KHALIDIAH-AD DIRIYAH", "AT-TURAIF-AD DIRIYAH", 
            "AL-THILAIMA-AD DIRIYAH", "AS-SALMANYAH-AD DIRIYAH", "AL-BLAIDA-AD DIRIYAH", "DIRAB", "BADER", "JABRAH", "MIDDLE AL-URAIJA", "DAHRAT LABAN", 
            "TWAEEQ", "AR-RAFEAH", "ISHBILIYAH", "AL-IMAM MOH-BIN-SAUD ISLAMIC UNIVERSITY", "AR-RIMAYAH", "Al Mashriq", "Al Bayan", "Al Marjan", 
            "Al Sahab", "UMM AL-SHAAL", "WEST UMM AL HAMAM", "AS-SARHIAH-AD DIRIYAH", "OLAISHAH", "Al Wasam", "DAHRAT-AL-ODAH-SHARQ-AD DIRIYAH", 
            "AL-OLAB-AD DIRIYAH", "AL-MALIK-AD DIRIYAH", "AS-SAADAH", "Al Bashaer", "ASH-SHOHDA-AD DIRIYAH", "QURAIWA-AD DIRIYAH", 
            "AL-FAISALIYAH-AD DIRIYAH", "AR-ROQIYA (SAMHAN)-AD DIRIYAH", "AL-ARID", "AN-NARJIS", "AL-QAIRAWAN", "TWAEEQ", "TWAEEQ", "BADER", 
            "AN-NADHEEM", "AD-DAR AL- BAIDA", "AD-DAR AL- BAIDA", "Laban", "NAMAR")

# Function
kuld <- function(inDIR, 
                 outPATH, 
                 pNAME, 
                 pattern, 
                 log = TRUE) {
  
  # Get system time for log file naming
  start_time = Sys.time()
  log_filename = paste0(format(start_time, "%Y%m%d_%H%M%S"), ".log")
  
  # Set up logging if requested
  if (log) {
    log_dir = file.path(outPATH, "log")
    if (!dir.exists(log_dir)) {
      dir.create(log_dir, recursive = TRUE)
      message("Created log directory: ", log_dir)
    }
    log_file = file.path(log_dir, log_filename)
    
    # Start logging - use message() instead of cli for logging
    con = file(log_file, "w")
    sink(con, append = TRUE, type = "output")
    sink(con, append = TRUE, type = "message")
    
    message("Logging started: ", log_file)
    message("Process started at: ", start_time)
    message("Input directory: ", inDIR)
    message("Output path: ", outPATH)
    message("Number of projects: ", length(pNAME))
  } else {
    cli::cli_alert_info("Process started at: {start_time}")
    cli::cli_alert_info("Input directory: {inDIR}")
    cli::cli_alert_info("Output path: {outPATH}")
    cli::cli_alert_info("Number of projects: {length(pNAME)}")
  }
  
  # Helper function for logging
  log_message = function(msg, type = "info") {
    if (log) {
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      message(paste0("[", timestamp, "] ", toupper(type), ": ", msg))
    } else {
      switch(type,
             "info" = cli::cli_alert_info(msg),
             "success" = cli::cli_alert_success(msg),
             "warning" = cli::cli_alert_warning(msg),
             "danger" = cli::cli_alert_danger(msg),
             message(msg)
      )
    }
  }
  
  maps = list.files(inDIR, pattern = pattern, full.names = T)
  log_message(paste("Number of maps found in 'inDIR':", length(maps)))
  
  total_moved = 0
  projects_processed = 0
  projects_with_files = 0
  
  for (pname in pNAME) {
    projects_processed = projects_processed + 1
    
    outDIR = file.path(outPATH, pname)
    maps_move = maps[grepl(pname, maps, ignore.case = TRUE)]
    
    if (length(maps_move) == 0) {
      log_message(paste("No files found for project:", pname), "warning")
      next
    }
    
    projects_with_files = projects_with_files + 1
    
    if (!dir.exists(outDIR)) {
      dir.create(outDIR, recursive = TRUE)
      log_message(paste("Created output directory:", outDIR))
    } else {
      log_message(paste("Existing output directory:", outDIR))
    }
    
    for (map_file in maps_move) {
      destination = file.path(outDIR, basename(map_file))
      
      if (file.copy(map_file, destination)) {
        log_message(paste("Copied:", basename(map_file), "->", pname, "/"), "success")
        total_moved = total_moved + 1
      } else {
        log_message(paste("Failed to copy:", basename(map_file)), "danger")
      }
    }
  }
  
  end_time = Sys.time()
  duration = end_time - start_time
  
  log_message("Operation completed!", "success")
  log_message(paste("Total projects processed:", projects_processed))
  log_message(paste("Projects with files found:", projects_with_files))
  log_message(paste("Total files moved:", total_moved))
  log_message(paste("Process duration:", round(duration, 2), attr(duration, 'units')))
  log_message(paste("Process ended at:", end_time))
  
  # Stop logging if it was enabled
  if (log) {
    sink(type = "message")
    sink(type = "output")
    if (exists("con")) close(con)
    message("Log file saved: ", log_file)
  }
}

# Usage examples:
# With logging (default)
kuld(inDIR = "D:/TMO/EPtets", 
     outPATH = "D:/TMO/EPtets/test", 
     pNAME = EPname, 
     pattern = ".pdf")

# Without logging
kuld(inDIR = "D:/TMO/EPtets", 
     outPATH = "D:/TMO/EPtets/test", 
     pNAME = EPname, 
     pattern = ".pdf",
     log = FALSE)



