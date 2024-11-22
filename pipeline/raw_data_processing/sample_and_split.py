import os
import shutil
from typing import List, Tuple
from tqdm import tqdm

def count_and_split_pdfs(source_dir: str, output_dir: str, max_files_per_subset: int = 10000) -> Tuple[int, List[str]]:
    """
    Count total number of PDF files in the source directory and split them into numbered subset folders in a new directory.

    Args:
        source_dir (str): Path to the source directory containing PDF files.
        output_dir (str): Path to the output directory where subset folders will be created.
        max_files_per_subset (int): Maximum number of PDF files per subset folder.

    Returns:
        Tuple[int, List[str]]: Total number of PDF files and list of created subset folder paths.
    """
    # Ensure the source directory exists
    if not os.path.isdir(source_dir):
        raise ValueError(f"Source directory '{source_dir}' does not exist.")

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get all PDF files in the source directory
    pdf_files: List[str] = [f for f in os.listdir(source_dir) if f.lower().endswith(".pdf")]
    total_pdfs: int = len(pdf_files)

    if total_pdfs == 0:
        print("No PDF files found in the source directory.")
        return 0, []

    # Calculate the number of subset folders needed
    num_subsets: int = (total_pdfs + max_files_per_subset - 1) // max_files_per_subset

    subset_folders: List[str] = []

    # Create a TQDM progress bar for the overall process
    with tqdm(total=total_pdfs, desc="Copying files", unit="file") as pbar:
        for i in range(num_subsets):
            subset_name: str = f"subset_{i + 1}"
            subset_path: str = os.path.join(output_dir, subset_name)
            
            # Create the subset folder
            os.makedirs(subset_path, exist_ok=True)
            subset_folders.append(subset_path)

            # Calculate the range of files for this subset
            start_idx: int = i * max_files_per_subset
            end_idx: int = min((i + 1) * max_files_per_subset, total_pdfs)

            # Copy PDF files to the subset folder
            for j in range(start_idx, end_idx):
                pdf_file: str = pdf_files[j]
                shutil.copy2(os.path.join(source_dir, pdf_file), os.path.join(subset_path, pdf_file))
                pbar.update(1)  # Update the progress bar

    print(f"\nTotal PDF files: {total_pdfs}")
    print(f"Number of subset folders created: {num_subsets}")
    
    return total_pdfs, subset_folders

if __name__ == "__main__":
    source_directory: str = "indo_journals"
    output_directory: str = "indo_journals_subsets"
    max_files_per_subset: int = 10000

    try:
        total_count, created_subsets = count_and_split_pdfs(source_directory, output_directory, max_files_per_subset)
        print("Subset folders created:")
        for subset in created_subsets:
            print(f"- {subset}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
