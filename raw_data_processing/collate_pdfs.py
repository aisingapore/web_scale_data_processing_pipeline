import os
import shutil
from typing import List, Optional, Tuple
import PyPDF2
from tqdm import tqdm
import concurrent.futures
import multiprocessing
import threading

def is_valid_pdf_with_content(file_path: str) -> bool:
    """
    Check if the file is a valid PDF with content.

    Args:
        file_path (str): The path to the PDF file.

    Returns:
        bool: True if the file is a valid PDF with content, False otherwise.
    """
    try:
        with open(file_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            if len(reader.pages) == 0:
                return False
            # Check if at least one page has content
            for page in reader.pages:
                if page.extract_text().strip():
                    return True
        return False
    except PyPDF2.errors.PdfReadError:
        return False
    except Exception:
        return False

def get_folder_list(source_dir: str) -> List[str]:
    """
    Get a list of all folders in the source directory.

    Args:
        source_dir (str): The path to the source directory.

    Returns:
        List[str]: A list of folder paths.
    """
    folder_list = []
    for root, dirs, _ in os.walk(source_dir):
        folder_list.append(root)
        for dir in dirs:
            folder_list.append(os.path.join(root, dir))
    return folder_list

def process_pdf(args: Tuple[str, str, str, threading.Event, threading.Lock, List[int], tqdm]) -> Optional[str]:
    """
    Process a single PDF file.

    Args:
        args (Tuple[str, str, str, threading.Event, threading.Lock, List[int], tqdm]): 
            A tuple containing (file, folder, output_dir, stop_event, lock, copied_files, process_pbar).

    Returns:
        Optional[str]: The destination path if the file was copied, None otherwise.
    """
    file, folder, output_dir, stop_event, lock, copied_files, process_pbar = args
    if stop_event.is_set():
        return None

    source_path = os.path.join(folder, file)
    
    if is_valid_pdf_with_content(source_path):
        dest_path = os.path.join(output_dir, file)
        
        # Handle duplicate filenames
        file_counter = 1
        while os.path.exists(dest_path):
            name, ext = os.path.splitext(file)
            dest_path = os.path.join(output_dir, f"{name}_{file_counter}{ext}")
            file_counter += 1
        
        # Copy the PDF file
        shutil.copy2(source_path, dest_path)
        with lock:
            copied_files[0] += 1
            process_pbar.update(1)
            process_pbar.set_description(f"Processing and copying PDFs (Copied: {copied_files[0]})")
        return dest_path
    else:
        with lock:
            process_pbar.update(1)
        return None

def collate_pdfs(source_dir: str, output_dir: str, max_files: Optional[int] = None) -> None:
    """
    Collate valid PDF files with content from the source directory and its subdirectories into the output directory.

    Args:
        source_dir (str): The path to the source directory containing PDFs.
        output_dir (str): The path to the output directory where PDFs will be copied.
        max_files (Optional[int]): Maximum number of files to copy. If None, copy all valid PDFs.

    Raises:
        FileNotFoundError: If the source directory doesn't exist.
        PermissionError: If there are permission issues accessing directories or files.
    """
    try:
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        folder_list = get_folder_list(source_dir)
        
        # Create a progress bar for folders
        with tqdm(total=len(folder_list), desc="Scanning folders", unit="folder") as folder_pbar:
            pdf_files = []
            for folder in folder_list:
                folder_pdf_files = [(file, folder, output_dir) for file in os.listdir(folder) if file.lower().endswith(".pdf")]
                pdf_files.extend(folder_pdf_files)
                folder_pbar.update(1)
                
                if max_files and len(pdf_files) >= max_files:
                    pdf_files = pdf_files[:max_files]
                    break

        tqdm.write(f"Found {len(pdf_files)} PDF files to process.")

        # Get the number of CPU cores
        max_workers = multiprocessing.cpu_count()

        stop_event = threading.Event()
        lock = threading.Lock()
        copied_files = [0]  # Use a list to make it mutable

        # Create a progress bar for processing and copying PDFs
        with tqdm(total=len(pdf_files), desc="Processing and copying PDFs", unit="file") as process_pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_pdf, (*args, stop_event, lock, copied_files, process_pbar)) for args in pdf_files]
                
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        tqdm.write(f"Copied: {result}")

        tqdm.write(f"\nProcess complete. Copied {copied_files[0]} files to '{output_dir}'")

    except FileNotFoundError as e:
        tqdm.write(f"Error: The source directory '{source_dir}' was not found.")
        raise e
    except PermissionError as e:
        tqdm.write(f"Error: Permission denied when accessing files or directories.")
        raise e
    except Exception as e:
        tqdm.write(f"An unexpected error occurred: {str(e)}")
        raise e

def main() -> None:
    """
    Main function to execute the PDF collation process.
    """
    source_directory: str = input("Enter the source directory path: ").strip()
    output_directory: str = os.path.join(os.getcwd(), "indo_journals")

    copy_all = input("Do you want to copy all valid PDFs? (y/n): ").strip().lower()
    max_files = None
    if copy_all != "y":
        while True:
            try:
                max_files = int(input("Enter the maximum number of PDFs to copy: "))
                if max_files > 0:
                    break
                else:
                    print("Please enter a positive number.")
            except ValueError:
                print("Please enter a valid number.")

    try:
        collate_pdfs(source_directory, output_directory, max_files)
        print(f"PDF collation complete. Files copied to '{output_directory}'")
    except Exception as e:
        print(f"PDF collation failed: {str(e)}")

if __name__ == "__main__":
    main()