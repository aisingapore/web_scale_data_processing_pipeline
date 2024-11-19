# Data Processing Instructions

1. Copy `subset_1_filtered_updated_final_output.csv` into the local folder
   - This is the raw output from the complete PDF extraction pipeline

2. Process the raw OCR output using `process_raw_output.ipynb`
   - Demonstrates REGEX application via `extract_meaningful_text` function
   - Pipeline:
     1. Filter pipeline errors from dirty web-scraped PDFs
     2. Apply REGEX to produce continuous training text
     3. Modify `extract_meaningful_text` function as needed for different outputs