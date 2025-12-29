#!/usr/bin/env Rscript

# Process NHANES 80Hz accelerometer data to UKBB format using GGIR
# Usage: Rscript process_ggir.R <input_dir> <output_dir>

# Load required library
suppressPackageStartupMessages({
  if (!require("GGIR", quietly = TRUE)) {
       install.packages('GGIR')
       stop("GGIR package not installed. Installed with: install.packages('GGIR')")
  }
})

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 2) {
  cat("Usage: Rscript process_ggir.R <input_csv_dir> <output_dir>\n")
  cat("\nArguments:\n")
  cat("  input_csv_dir  : Directory containing CSV files with raw 80Hz accelerometer data\n")
  cat("  output_dir     : Directory where GGIR output will be saved\n")
  cat("\nExample:\n")
  cat("  Rscript process_ggir.R ./nhanes_data ./ggir_output\n")
  quit(status = 1)
}

input_dir <- args[1]
output_dir <- args[2]

# Validate input directory
if (!dir.exists(input_dir)) {
  stop(paste("Input directory does not exist:", input_dir))
}

# Create output directory if it doesn't exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
  cat("Created output directory:", output_dir, "\n")
}

# Display processing info
cat("\n======================================\n")
cat("GGIR Processing: NHANES to UKBB Format\n")
cat("======================================\n")
cat("Input directory :", input_dir, "\n")
cat("Output directory:", output_dir, "\n")
cat("Target format   : UKBB (5-second ENMO epochs)\n")
cat("======================================\n\n")

# Run GGIR Part 1
cat("Starting GGIR Part 1 processing...\n\n")

tryCatch({
  GGIR(
      datadir = input_dir,
      outputdir = output_dir,
      mode = c(1, 2), # Only run Part 1
      do.report = c(1, 2), # Generate Part 1 report
      overwrite = TRUE, # Don't overwrite existing files
      windowsizes = c(5, 900, 3600),
      studyname = "NHANES",  # Add this line
      # UKBB-specific parameters
      desiredtz = "UTC",           # Use UTC timezone
      # Data format specifications
      dataFormat = "raw",          # Input format

      # ENMO calculation (UKBB standard)
      do.enmo = TRUE,              # Calculate ENMO metric
      do.anglez = FALSE,           # Calculate angle-z (useful for orientation)
      epochvalues2csv = TRUE,

      # Additional useful parameters
      printsummary = TRUE,         # Print summary to console
      do.parallel = TRUE,           # Use parallel processing if available
      # rmc.check4timegaps = TRUE,
      # chunksize = 0.5,
      # NHANES specific params
      # rmc.nrow                 = Inf,
      # rmc.skip                 = 0,
      rmc.dec                  = ".",
      rmc.firstrow.acc         = 2,
      rmc.col.acc              = 2:4,
      rmc.col.time             = 1,
      rmc.unit.acc             = "g", # Confirmed, it's g not bit not mg
      rmc.unit.time            = "POSIX", # Has to be POSIX. If use char then SPT will be messed up (like the result from asleep)
      rmc.format.time          = "%Y-%m-%d %H:%M:%OS", # Changed to OS however I don't think there's a difference 
      rmc.dynamic_range        = 6, # https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2011/DataFiles/PAX80_G.htm, this is for clipping (unusual high acceleration)
      rmc.sf                   = 80, #  Freq
      #rmc.firstrow.header = 1,     # Header is row 1
      #rmc.header.length = 1,       # One header row
  )
  
  cat("\n======================================\n")
  cat("GGIR Processing Complete!\n")
  cat("======================================\n")
  cat("Output location:", file.path(output_dir, "meta", "basic"), "\n")
  cat("Files to look for: meta_*.RData containing 5-second ENMO epochs\n")
  cat("======================================\n\n")
  
}, error = function(e) {
  cat("\nERROR during GGIR processing:\n")
  cat(conditionMessage(e), "\n")
  quit(status = 1)
})

cat("Script completed successfully.\n")