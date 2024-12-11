import argparse
import logging
import markdown2
import os
import pandas as pd
import re
from bs4 import BeautifulSoup
from langchain_text_splitters import MarkdownHeaderTextSplitter
from transformers import AutoTokenizer
import time
from utils import EXCLUDED_PDF_PATHS
from tqdm import tqdm
import signal
tqdm.pandas()


class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

class ProcessIndoJournal:
    def __init__(
        self,
        input_path: str,
        output_dir: str,
        output_file_name: str,
        min_token: int,
    ):
        self.input_path = input_path
        self.output_dir = output_dir
        self.output_file_name = output_file_name
        self.min_token = min_token
        self.markdown_splitter = MarkdownHeaderTextSplitter([
            ("#", "Header 1"),
            ("##", "Header 2"),
            ("###", "Header 3"),
            ("####", "Header 4"),
            ("#####", "Header 5"),
            ("######", "Header 6"),
        ])
        self.tokenizer = AutoTokenizer.from_pretrained("GoToCompany/llama3-8b-cpt-sahabatai-v1-base")

    def _markdown_to_text(self, md_content):
        """Convert Markdown content to plain text."""
        try:
            html = markdown2.markdown(md_content)
            soup = BeautifulSoup(html, "html.parser")
            return soup.get_text()
        except Exception as e:
            logger.error(f"Error converting markdown to text, skipping this MD section: {e}")
            return ""

    def get_section_text(self, section):
        if not section.metadata:
            return self._markdown_to_text(section.page_content)
        if re.search(r'^\d+$', section.metadata['Header']):
            return self._markdown_to_text(section.page_content)
        return f"## {self._markdown_to_text(section.metadata['Header']).strip()}\n\n{self._markdown_to_text(section.page_content)}"

    def markdown_to_text(self, row):
        
        # Set the signal handler and a 60-second alarm
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(60)
        
        try:
            md_split = self.markdown_splitter.split_text(row.md_extraction_result)

            # Replace metadata to only contain the lowest level header.
            for section in md_split:
                if section.metadata:
                    tmp_header = {int(k.replace("Header ", "")): v for k, v in section.metadata.items()}
                    section.metadata = {"Header": tmp_header[max(tmp_header.keys())].strip()}

            return "\n\n".join([self.get_section_text(section) for section in md_split])
        except TimeoutException:
            logger.error(f"Timeout error. Skipping the file: {row.pdf_path}")
            return ""
        finally:
            signal.alarm(0)

    def apply_regex(self, markdown_content):
        content = '\n'.join(line.strip() for line in markdown_content.split('\n'))

        # Remove markdown tables (improved version)
        content = re.sub(r'\|[^\n]*\|(\n\|[-:| ]+\|)?(\n\|[^\n]*\|)*', '', content)

        # Remove figure and table captions
        content = re.sub(r'^(Gambar|Figure) \d+[^\S\n\r]*?(\.|:).*?\n', '', content, flags=re.MULTILINE)
        content = re.sub(r'^(Tabel|Table) \d+[^\S\n\r]*?(\.|:).*?\n', '', content, flags=re.MULTILINE)
        
        # Remove keywords section
        content = re.sub(r'(?i)^(key ?words?|(kata-)?kata kunci)[^\S\r\n]*(:|—|-).*?\n', '', content, flags=re.MULTILINE)
        content = re.sub(r'(?i)(key ?words?|(kata-)?kata kunci)[^\S\r\n]*(:|—|-).*?\n', '\n', content)

        # Remove URLs
        content = re.sub(r'https?:\/\/[a-zA-Z0-9.-]+(:[0-9]+)?(\/[^\s]*)?', '', content)

        # Remove email addresses
        content = re.sub(r'(?i)(e-?mail ?: )?[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', content)

        # Remove ISSN code
        content = re.sub(r'(e-|p-|e |p |e|p)?ISSN \d{4} ?- ?\d{3}[\dX]', '', content)

        # Remove correspondence address
        content = re.sub(r'(?i)alamat korespondensi\s*:(?:[^\n]*\n?){0,2}[^\n]* \d{5}', '', content, flags=re.MULTILINE)
        content = re.sub(r'^alamat korespondensi\s*:.*\n', '', content, flags=re.MULTILINE | re.IGNORECASE)

        # Remove lines that only contain non alphabetic characters
        content = re.sub(r'^[^a-zA-Z]+\n', '', content, flags=re.MULTILINE)

        # Remove lines starting with ©
        content = re.sub(r'^©.*\n', '', content, flags=re.MULTILINE)

        content = content.strip()
        
        return content

    def move_abstract_as_header(self, text):
        pattern = r'\bA(bstrak|bstract)\b'
        md_split = self.markdown_splitter.split_text(text)

        if (
            len(md_split) > 1 and re.search(pattern, md_split[0].page_content, flags=re.IGNORECASE)
            or len(md_split) > 2 and re.search(pattern, md_split[1].page_content, flags=re.IGNORECASE)
            or len(md_split) > 3 and re.search(pattern, md_split[2].page_content, flags=re.IGNORECASE)
        ):
            return re.sub(r'\bA(bstrak|bstract)\b[^a-zA-Z0-9]*', r'\n\n## A\1\n\n', text, flags=re.IGNORECASE)

        return text

    def split_md_sections(self, raw_text):
        md_split = self.markdown_splitter.split_text(raw_text)
        # Replace metadata to only contain the lowest level header.
        for section in md_split:
            if section.metadata:
                tmp_header = {int(k.replace("Header ", "")): v for k, v in section.metadata.items()}
                section.metadata = {"Header": tmp_header[max(tmp_header.keys())]}
        return md_split

    def abstract_location(self, md_sections):
        for i, section in enumerate(md_sections):
            if section.metadata and re.search(r'\bA(?i:bstrak|bstract)\b', section.metadata["Header"]):
                return i
        return -1

    def intro_location(self, md_sections):
        for i, section in enumerate(md_sections):
            if section.metadata and re.search(r'\b(P|I)(?i:endahuluan|ntroduction)\b', section.metadata["Header"]):
                return i
        return -1

    def include_section(self, section_no, section, abstract_location, intro_location):
        # Exclude sections with certain headers.
        if section.metadata:
            header = section.metadata["Header"]
            if any(text in header for text in ["Daftar", "Editor", "Reference", "Referensi", "Bibliografi", "Bibliography"]):
                return False
            elif "info" in header.lower() and ("artikel" in header.lower() or "article" in header.lower()):
                return False
            elif any(text in header.lower() for text in ["how to cite", "sejarah artikel", "article history"]):
                return False

        # Exclude all sections before "abstract" if there is abstract section (mostly just author information).
        if abstract_location >= 0 and abstract_location <= 3 and section_no < abstract_location:
            return False

        # Exclude all sections before "introduction" if there is no abstract section (mostly just author information).
        if abstract_location < 0 and intro_location >= 0 and intro_location <= 3 and section_no < intro_location:
            return False

        return True

    def get_final_output(self, row):
        arr = []
        for i, section in enumerate(row['md_sections']):
            if self.include_section(i, section, row['abstract_location'], row['intro_location']):
                if not section.metadata:
                    arr.append(section.page_content)
                else:
                    arr.append(f"{section.metadata['Header']}\n\n{section.page_content}")
        return "\n\n".join(arr)
    
    def run(self):
        print("running")
        logger.info(f'Start processing Indo journals in {self.input_path}...')

        # Read CSV files in input_path
        if self.input_path.endswith('.csv'):
            df = pd.read_csv(self.input_path)
        else:
            csv_files = [
                os.path.join(self.input_path, file) for file in os.listdir(self.input_path) if file.endswith('.csv')
            ]
            df = pd.concat((pd.read_csv(file) for file in csv_files), ignore_index=True)
        logger.info(f'Original df shape = {df.shape}')

        # Filter out some files
        df = df[
            (df["pdf_path"].notna()) & 
            #(df["pdf_path"].str.startswith("/home/ubuntu/")) & 
            (df["md_extraction_result"].notna()) &
            (~df["md_extraction_result"].str.startswith("Error", na=False)) &
            (~df["pdf_path"].isin(EXCLUDED_PDF_PATHS))
            #(~df['md_extraction_result'].str.contains('Certicate of Publication is awarded to', case=False, na=False))
        ]
        df.drop_duplicates(subset='md_extraction_result', keep='first', inplace=True)
        df.reset_index(drop=True, inplace=True)
        logger.info(f'After filtering = {df.shape}')

        # Process extracted text
        logger.info("Removing markdown format...")
        df['extraction_result'] = df.progress_apply(self.markdown_to_text, axis=1)
        logger.info("Applying regex...")
        df['applied_regex'] = df['extraction_result'].progress_apply(self.apply_regex)
        df['applied_regex'] = df['applied_regex'].progress_apply(self.move_abstract_as_header)
        logger.info("Splitting files into MD sections...")
        df['md_sections'] = df['applied_regex'].progress_apply(self.split_md_sections)
        df['abstract_location'] = df['md_sections'].progress_apply(self.abstract_location)
        df['intro_location'] = df['md_sections'].progress_apply(self.intro_location)
        logger.info("Generating final output...")
        df['extracted_meaningful_text_v2'] = df.progress_apply(self.get_final_output, axis=1)

        # Filter short files (based on number of tokens)
        logger.info("Counting number of tokens...")
        df['n_tokens'] = df['extracted_meaningful_text_v2'].progress_apply(
            lambda x: self.tokenizer(x, return_tensors='pt')['input_ids'].shape[1]
        )
        df = df[df['n_tokens'] > self.min_token]
        df.reset_index(drop=True, inplace=True)
        logger.info(f"df shape after removing short files = {df.shape}")
        logger.info(f"{df.n_tokens.describe()}")
        logger.info(f"Total number of tokens = {df['n_tokens'].sum()}")

        # Save result
        df[['pdf_path', 'md_extraction_result', 'extracted_meaningful_text_v2', 'n_tokens']].sample(100).to_csv(
            f"{self.output_dir}/{self.output_file_name}_sampled.csv", index=False)
        df[['pdf_path', 'md_extraction_result', 'extracted_meaningful_text_v2', 'n_tokens']].to_csv(
            f"{self.output_dir}/{self.output_file_name}.csv", index=False)
        logger.info(f"Result saved to {self.output_dir}/{self.output_file_name}.csv")


# python process_indo_journal.py --input_path outputs_raw/subset_36_47 --output_dir results --output_file_name subset_36_47
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        type=str,
        help="Path to the directory of CSV files",
        required=True,
    )
    parser.add_argument(
        "--output_dir", type=str, help="Path to output", required=True
    )
    parser.add_argument(
        "--output_file_name", type=str, help="Result filename without extension", required=True
    )
    parser.add_argument(
        '--min_token', type=int, default=300,
        help='Minimum token count for a file to be included'
    )
    args = parser.parse_args()

    
    print("starting")
    ProcessIndoJournal(args.input_path, args.output_dir, args.output_file_name, args.min_token).run()
