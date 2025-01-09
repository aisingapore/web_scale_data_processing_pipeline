# Processing outputs from raw web scraping results (raw_data_processing dir)

* This pipeline assumes access to ```indo_journals_subsets``` directory, containing 47 subsets of ~10K PDFs each
    * can refer to ```collate_pdfs.py``` which processes raw ```/data``` directory containing web scrape output, which includes nested folders, duplicate files, irrelevant files (.txt, .html)
    * output is indo_journals directory containing ONLY valid PDF files with at least 1 page that can be loaded
    * run ```sample_and_split.py``` to split single directory of PDFs into subsets of 10K PDFs each, to get the indo_journals_subsets directory

# Quality Filtering

* not all PDFs contain Indonesian only, some are in English and also a mix of both. For now, we want predominantly Indonesian-only journals used for Continous PreTraining (CPT)
* To filter out Indonesian-only (defined as at least 80% of text in PDF) journals, we use a smart LLM with decent Indonesian performance, and reasonable price. Recommended GPT4o-mini (cheaper and won't get rate limitted). Claude-3-5-sonnet is more expensive and will get rate limitted.

1. use ```filter_pdfs.py``` in ideally a machine with many cores to maximises PySpark parallelism
    * test on small number of PDFs first before running on complete subset. Use ```indo_journals_sample``` directory for this
2. In ```filter_pdfs.py```, notice prompt used for language detection (to filter out 80% Indonesian text) can be improved but it works reasonably well. Note we also sample only the first 15K characters of the PDF, which so far is reasonable but do check outputs for edge cases
    * could use FastText for language detection too, though LLM-based quality filtering is more commonly used now, even in LLAMA. 
    * ideally using self-hosted Llama for this step could potentially save on API costs
    * if Claude is used, note that extracting the answer by REGEX is required because we prompt Claude to generate chain of thought before determining final answer
3. also chose to filter for PDFs with page count between 2 and 500, as some PDFs are 1-page cover pages (not semantically useful). 500 is an arbitrary number, for edge cases like 1K page PDFs which might create a bottleneck for whole pipeline (should do analysis on number of pages)
4. The output of this pipeline is ```subset_n_filtered_updated.csv```, which contains a ```is_relevant``` boolean field produced by the LLM-based language detection filter in step #2.
    * ocr_text is raw OCR output from a cheap, OCR job using PyPDF, which does not parse sections required for generating continuous text for CPT

# Text Extraction using Marker Engine for structured OCR output

* to get the cleanest possible OCR output with the least noise, a OCR engine which identifies and parses section contents well is required
* with each section header cleanly parsed, we know the content within it will be cleanly extracted. This output will have minimal noise.
* the best OCR that parses section contents well is MathPix, but is expensive when accessed by API, which adds up for large scale extractions. 
* Marker-pdf is an open source OCR engine able to parse sections reasonably well. Price more reasonably since paying for price per GPU/hour. 
    * 1 subset uses 1xH100 node for ~3 hours
* use ```marker_spark_pipeline_single_gpu.py``` which wraps marker (requiring single GPU to run) with PySpark to maximise throughput of large scale PDF extraction and include REGEX filtering based on FineWeb Edu
* output will be ```subset_n_final_output.csv```, which follows most closely to the outputs of [fineweb-edu](https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu/viewer/sample-10BT/train?q=arxiv&row=379320) arxiv 

## Extensions
* VLMs are commonly used not for OCR, could explore
