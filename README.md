# Data Processing Pipeline

# Description
The complete data pipeline is found in the ```pipeline``` directory, which provides an implementation used to extract text from raw scraped outputs from open web journals. Depending on your starting point, the pipeline is easily adaptable due to its modular nature. 

# Pipeline
1. Filtering from web scraped outputs
* Web scraped outputs usually contain unwanted files (i.e. .html, .js files), first we need to filter them, which is done using the ```pipeline/raw_data_processing``` step. 

2. Quality Filtering
* Depending on use case, you might want to filter and sort the types of content accordingly
* In this case, our objective is to improve Indonesian performance using pretraining, so we focus on filtering for Indonesian data from the open web source
* Inspired by modern data filtering techniques used by [FineWeb Edu](https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu), we adopt a simplified LLM-based quality filtering, using GPT4o-mini as the filtering model for its reasonable performance in Indonesian
   * Refer to the prompt provided in ```quality_filtering/filter_pdfs.py``` for more details
   * As we are doing extraction at scale, only the first 15000 characters are considered in this filtering step as an arbitrary figure to prevent API costs from blowing up
   * Depending on your context, you should also consider including context specific heuristics-based filtering, like how we filtered out PDFs with only 1 page, which are known to be cover pages with little semantic value
* Older techniques use a two-step process to filter high quality data: firstly, a language detection step using [fastText](https://github.com/facebookresearch/fastText) or equivalent which supports your desired language; then with tailored quality filtering models such as [KenLM](https://www.arxiv.org/abs/2409.09613).
* You can perform a document level and global level deduplication using algorithms like [minHash](https://ekzhu.com/datasketch/lsh.html) and libraries like [Trafilatura](https://trafilatura.readthedocs.io/en/latest/)
  
3. OCR Text Extraction
