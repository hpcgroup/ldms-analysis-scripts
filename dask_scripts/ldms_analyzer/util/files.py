import glob
import os

def get_files(directory_path, patterns, metrics):
    ldms_files = []
    sacct_files = []

    # List all files in the directory
    all_files = glob.glob(os.path.join(directory_path, "*.pq"))

    # Filter files based on the specified patterns
    for pattern in patterns:
        for metric in metrics:
            ldms_pattern = f"*{metric}*{pattern}*_ldms.pq"
            saact_pattern = f"*{metric}*{pattern}*_saact.pq"

            ldms_files.extend(glob.glob(os.path.join(directory_path, ldms_pattern)))
            sacct_files.extend(glob.glob(os.path.join(directory_path, saact_pattern)))
    
    return ldms_files, sacct_files