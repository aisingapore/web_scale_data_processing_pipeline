1. Copy ```subset_1_filtered_updated_final_output.csv``` into the local folder, this is the raw output from the complete PDF extraction pipeline
2. In ```process_raw_output.ipynb```, we demonstrate how different REGEX can be applied to the raw OCR output, see function: extract_meaningful_text
    a. First, we filter out errors that from the pipeline, there are dirty PDFs from the web scrape
    b. Then we apply the REGEX to produce continuous text for training
    c. change ```extract_meaningful_text``` function in ```process_raw_output.ipynb``` for a different output 