import csv
import os
import json
import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, trim
from pyspark.sql.types import StringType
import re
import urllib.parse
import logging
from marker.convert import convert_single_pdf
from marker.models import load_all_models
from marker.output import save_markdown
import tempfile
import shutil
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load all models once
model_lst = load_all_models()

NUM_WORKERS = 4  # n(max_memory) / 2*(4.1 GB VRAM per Marker worker) e.g. 1x A100 (multi-GPU not multi-node) 40GB // 8.2GB = 5 max workers

def process_pdf(pdf_path, parallel_factor=NUM_WORKERS):
    """Process a PDF file using marker's convert_single_pdf function and return the MD content."""
    # Check if pdf_path is None
    if pdf_path is None:
        logger.error("Received None as pdf_path")
        return "Error: PDF path is None"
    
    # Remove the 'file:' prefix if present
    if isinstance(pdf_path, str) and pdf_path.startswith("file:"):
        pdf_path = urllib.parse.unquote(pdf_path[5:])
    
    try:
        full_text, images, out_meta = convert_single_pdf(pdf_path, model_lst)
        
        # Generate a unique filename
        unique_filename = f"{uuid.uuid4().hex}_{os.path.basename(pdf_path)}"
        
        # Save the markdown to the current directory
        current_dir = os.getcwd()
        md_file_path = save_markdown(current_dir, unique_filename, full_text, images, out_meta)

        # Assuming save_markdown creates a directory
        md_dir = save_markdown(current_dir, unique_filename, full_text, images, out_meta)
        
        # Construct the path to the .md file inside the created directory
        md_filename = os.path.basename(md_dir) + ".md"
        md_file_path = os.path.join(md_dir, md_filename)
        
        # Read the content of the markdown file
        with open(md_file_path, "r", encoding="utf-8") as md_file:
            md_content = md_file.read()
        
        # Remove the temporary markdown file
        os.remove(md_file_path)
        
        # Remove the temporary directory containing the markdown file
        shutil.rmtree(md_dir)
        
        return md_content
    except Exception as e:
        logger.error(f"Error processing PDF {pdf_path}: {str(e)}")
        return f"Error: Failed to process PDF {pdf_path}"

def extract_meaningful_text(markdown_content):
    """Extract meaningful text from markdown content."""
    # Remove metadata and formatting
    content = re.sub(r'^---.*?---', '', markdown_content, flags=re.DOTALL)
    
    # Remove title tags but keep the title text
    content = re.sub(r'^#\s*(.*?)\n', r'\1\n', content)
    
    # Remove #### symbols but keep the header content
    content = re.sub(r'^####\s*(.*?)\n', r'\1\n', content, flags=re.MULTILINE)
    content = re.sub(r'^##\s*(.*?)\n', r'\1\n', content, flags=re.MULTILINE)
    
    content = re.sub(r'\*\*.*?\*\*', '', content)
    content = re.sub(r'\$.*?\$', '', content)
    
    # Remove citations and references
    content = re.sub(r'\[.*?\]', '', content)
    content = re.sub(r'\(.*?\)', '', content)
    
    # Remove figure captions and table content
    content = re.sub(r'Gambar \d+\..*', '', content)
    
    # Remove Markdown tables (improved version)
    content = re.sub(r'\|[^\n]*\|(\n\|[-:| ]+\|)?(\n\|[^\n]*\|)*', '', content)
    
    # Remove Keywords section
    content = re.sub(r'Keywords:.*?(?=\n\n)', '', content, flags=re.DOTALL)
    
    # Remove extra whitespace and newlines
    content = re.sub(r'\s+', ' ', content)
    content = content.strip()
    
    return content

# Create Spark session with increased memory
spark = (SparkSession.builder
         .appName("MathpixPDFProcessing")
         .config("spark.driver.memory", "8g")
         .config("spark.executor.memory", "8g")
         .config("spark.memory.offHeap.enabled", "true")
         .config("spark.memory.offHeap.size", "8g")
         .config("spark.executor.cores", str(NUM_WORKERS))  # Set the number of cores per executor
         .config("spark.executor.instances", "2")  # Adjust based on your cluster setup
         .getOrCreate())

# Check the Spark UI for stage information
print(f"Check the Spark UI at: {spark.sparkContext.uiWebUrl}")

# Define UDF for PDF processing
process_pdf_udf = udf(lambda path: process_pdf(path, NUM_WORKERS), StringType())
extract_meaningful_text_udf = udf(extract_meaningful_text, StringType())

# Start timing the entire process
start_time_total = time.time()

# Load CSV file
df = spark.read.csv("/root/projects/indo-journal-pipeline/sample_filtered_latest_25_pdf_path.csv", header=True, inferSchema=True)

# Filter rows where is_relevant is True
filtered_df = df.filter(col("is_relevant") == True)

# Get the total number of rows to process
total_rows = filtered_df.count()
logging.info(f"Total rows to process: {total_rows}")

# Step 1: Apply the process_pdf_udf
logging.info("Starting Step 1: process_pdf_udf")
start_time_step1 = time.time()
result_df_step1 = (filtered_df
                   .repartition(NUM_WORKERS * 2)  # Increase partitions for better parallelism
                   .withColumn("md_extraction_result", process_pdf_udf(col("pdf_path"))))
result_df_step1.cache()
count_step1 = result_df_step1.count()
end_time_step1 = time.time()
duration_step1 = end_time_step1 - start_time_step1
logging.info(f"Completed Step 1. Row count: {count_step1}. Duration: {duration_step1:.2f} seconds")

# Step 2: Apply the extract_meaningful_text_udf
logging.info("Starting Step 2: extract_meaningful_text_udf")
start_time_step2 = time.time()
result_df_step2 = result_df_step1.withColumn("extracted_meaningful_text", extract_meaningful_text_udf(col("md_extraction_result")))
count_step2 = result_df_step2.count()
end_time_step2 = time.time()
duration_step2 = end_time_step2 - start_time_step2
logging.info(f"Completed Step 2. Row count: {count_step2}. Duration: {duration_step2:.2f} seconds")

# Collect the results to the driver node
start_time_collect = time.time()
results = result_df_step2.collect()
end_time_collect = time.time()
duration_collect = end_time_collect - start_time_collect
logging.info(f"Collected results. Duration: {duration_collect:.2f} seconds")

# Write the results to a single CSV file
start_time_write = time.time()
row_count = 0
with open("/root/projects/indo-journal-pipeline/sample_filtered_latest_25_final_output.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["pdf_path", "page_count", "md_extraction_result", "extracted_meaningful_text"])  # Write header
    for row in results:
        writer.writerow([row["pdf_path"], row["page_count"], row["md_extraction_result"], row["extracted_meaningful_text"]])
        row_count += 1

end_time_write = time.time()
duration_write = end_time_write - start_time_write
logging.info(f"Wrote results to CSV. Duration: {duration_write:.2f} seconds")

# Calculate total duration
end_time_total = time.time()
duration_total = end_time_total - start_time_total

print(f"Results saved to: /root/projects/indo-journal-pipeline/sample_filtered_latest_25_final_output.csv")
print(f"Total number of rows in the CSV (including header): {row_count + 1}")
print(f"Total duration: {duration_total:.2f} seconds")

# Calculate and print time estimates
avg_time_per_row = duration_total / total_rows
estimated_time_1000_rows = avg_time_per_row * 1000
estimated_time_10000_rows = avg_time_per_row * 10000

print(f"Average time per row: {avg_time_per_row:.2f} seconds")
print(f"Estimated time for 1,000 rows: {estimated_time_1000_rows:.2f} seconds ({estimated_time_1000_rows/60:.2f} minutes)")
print(f"Estimated time for 10,000 rows: {estimated_time_10000_rows:.2f} seconds ({estimated_time_10000_rows/60:.2f} minutes)")

# Stop the Spark session
spark.stop()