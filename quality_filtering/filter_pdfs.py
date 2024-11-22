import csv
import os
from openai import OpenAI
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, rand
from pyspark.sql.types import StringType, IntegerType, BooleanType, StructType, StructField
from pypdf import PdfReader
import json
import anthropic
import re
from urllib.parse import unquote

# Initialize Spark session
spark = SparkSession.builder.appName("PDF OCR Pipeline").getOrCreate()

# Set up Anthropic client
anthropic_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

# Define the path to the indo_journals directory
# indo_journals_path = "/data/users/brandon/ob1-projects/data_processing/indo_journals_sample"
indo_journals_path = "/data/users/brandon/ob1-projects/data_processing/indo_journals_subsets/subset_4"

# Define schema for the initial DataFrame
pdf_schema = StructType([
    StructField("pdf_path", StringType(), True)
])

# Step 1: List all PDF files using Spark
def list_pdf_files(spark, directory):
    """List all PDF files in the given directory using Spark."""
    return spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf").load(directory).select("path")

pdf_df = list_pdf_files(spark, indo_journals_path)
print(f"Number of PDF files found: {pdf_df.count()}")

# Helper function to clean file paths
def clean_file_path(file_path):
    if file_path.startswith("file:"):
        file_path = file_path[5:]  # Remove "file:" prefix
    return unquote(file_path)  # Decode URL-encoded characters

# Step 2: Function to get the number of pages in a PDF
@udf(returnType=IntegerType())
def get_pdf_page_count(pdf_path):
    try:
        clean_path = clean_file_path(pdf_path)
        with open(clean_path, "rb") as file:
            pdf_reader = PdfReader(file)
            return len(pdf_reader.pages)
    except Exception as e:
        print(f"Error in get_pdf_page_count for {clean_path}: {str(e)}")
        return 0

# Step 3: Perform OCR on PDFs
@udf(returnType=StringType())
def perform_ocr(pdf_path):
    try:
        clean_path = clean_file_path(pdf_path)
        with open(clean_path, "rb") as file:
            pdf_reader = PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
        return text
    except Exception as e:
        print(f"Error in perform_ocr for {clean_path}: {str(e)}")
        return f"Error processing {clean_path}: {str(e)}"

def extract_output(text):
    pattern = r'<output>(.*?)</output>'
    match = re.search(pattern, text, re.DOTALL)
    return match.group(1).strip() if match else text

# Step 4: Use Anthropic LLM to determine if the PDF is relevant
@udf(returnType=BooleanType())
def is_relevant_pdf(pdf_text):
    try:
        # client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        MODEL="gpt-4o-mini"
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

        prompt = f"""You are an expert Language Identification Detector. Your task is to determine if the primary content of a research paper is in Indonesian based on its OCR output. This task is crucial for training a Large Language Model to improve its Indonesian language performance.

Here is the OCR output of the research paper:

<ocr_output>
{pdf_text[:15000]}
</ocr_output>

Carefully analyze the OCR output and determine if the primary content is in Indonesian. Consider the following guidelines:

1. The majority of the text should be in Indonesian.
2. Technical terms, citations, or references in English are acceptable as long as they don't make up a significant portion of the content.
3. If the paper contains substantial sections in English (e.g., abstract, conclusion, or entire paragraphs) that are difficult to remove via REGEX scripts, consider the paper inappropriate for training the Indonesian LLM.
4. Be aware that OCR errors might affect some words, but focus on the overall language pattern.

If you encounter any English content that is more than just scattered words or phrases, and would be difficult to remove with simple REGEX scripts, consider the paper unsuitable for training the Indonesian LLM.

Provide your analysis and reasoning in the "reasoning" key of the JSON output. Then, based on your analysis, determine whether the paper is suitable (true) or unsuitable (false) for training the Indonesian LLM, and include this in the "answer" key of the JSON output.

Your output should be in the following JSON format:

<output>
{{
  "reasoning": "Your detailed analysis and reasoning here",
  "answer": true/false
}}
</output>

Remember to provide your reasoning before giving the final answer. Ensure your reasoning is thorough and supports your conclusion."""

        # message = client.messages.create(
        #     model="claude-3-5-sonnet-20240620",
        #     max_tokens=1000,
        #     temperature=0,
        #     messages=[
        #         {
        #             "role": "user",
        #             "content": [
        #                 {
        #                     "type": "text",
        #                     "text": prompt
        #                 }
        #             ]
        #         }
        #     ]
        # )
        completion = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )


        # response_content = message.content[0].text
        response_content = completion.choices[0].message.content
        print(f"API Response: {response_content}")
        # output_content = extract_output(response_content)
        # print(f"Extracted output: {output_content}")
        
        try:
            # cleaned_output = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', output_content)
            parsed_response = json.loads(response_content)
            return parsed_response["answer"]
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            print(f"Raw output: {response_content}")
            return "true" in response_content.lower()
    except Exception as e:
        print(f"Error in is_relevant_pdf: {str(e)}")
        return False

# Apply transformations using Spark DataFrame operations
df_with_page_count = pdf_df.withColumn("page_count", get_pdf_page_count(col("path")))
print(f"After adding page count: {df_with_page_count.count()}")

df_filtered = df_with_page_count.filter((col("page_count") > 2) & (col("page_count") < 500))
print(f"After filtering by page count: {df_filtered.count()}")

df_with_ocr = df_filtered.withColumn("ocr_text", perform_ocr(col("path")))
print(f"After performing OCR: {df_with_ocr.count()}")

result_df = df_with_ocr.withColumn("is_relevant", is_relevant_pdf(col("ocr_text")))
print(f"After relevance check: {result_df.count()}")

# Sample the DataFrame
# sampled_df = result_df.orderBy(rand()).limit(5)

# Step 5: Save the resulting DataFrame as a single CSV file
# output_path = "/data/users/brandon/ob1-projects/data_processing/sample_filtered.csv"
output_path = "/data/users/brandon/ob1-projects/data_processing/subset_4_filtered_updated.csv"

# Collect the results to the driver node
results = result_df.collect()

# Write the results to a single CSV file
row_count = 0
with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["pdf_path", "page_count", "is_relevant", "ocr_text"])  # Write header
    for row in results:
        writer.writerow([row["path"], row["page_count"], row["is_relevant"], row["ocr_text"]])
        row_count += 1

print(f"Results saved to: {output_path}")
print(f"Total number of rows in the CSV (including header): {row_count + 1}")

# Stop the Spark session
spark.stop()