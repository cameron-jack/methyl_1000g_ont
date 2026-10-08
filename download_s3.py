import boto3
from botocore import UNSIGNED
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

import sys
import os
from pathlib import Path
from argparse import ArgumentParser as AP


def download_large_file(s3_client, bucket_name: str, object_key: str, download_path: Path):
    print(f"Starting download: {object_key} -> {download_path}")
    # 1. Get the total file size in bytes
    meta_data = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    total_size = int(meta_data.get('ContentLength', 0))
    
    # Optimized configuration for 8 GB files on a local server
    dl_config = TransferConfig(
        multipart_threshold=1024 * 1024 * 64,  # 64 MB: Files larger than this use multipart
        multipart_chunksize=1024 * 1024 * 16,  # 16 MB: Download chunks size
        max_concurrency=15,                    # 15 parallel threads downloading chunks
        use_threads=True
    )
    
    pending_file = str(download_path) + ".part"
    # 2. Initialize the tqdm progress bar
    with tqdm(total=total_size, unit='B', unit_scale=True, desc=object_key, ncols=80) as pbar:
        # 3. Pass a lambda or function that updates tqdm on each chunk callback
        try:
            s3_client.download_file(
                Bucket=bucket_name,
                Key=object_key,
                Filename=pending_file,
                Config=dl_config,
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
            )
        except Exception as e:
            print(f"Error downloading {object_key}: {e}")
            if os.path.exists(pending_file):
                os.remove(pending_file)  # Clean up the pending file on error
            return
    # Rename the pending file to the final download path
    os.rename(pending_file, str(download_path))
    print(f"Download complete: {object_key}")


def list_s3_directories(client, bucket_name, prefix="", region_name=""):
    """
    Lists 'directories' (common prefixes) inside an S3 bucket under a specific prefix.
    """
    s3_client = client

    # Ensure the prefix ends with a slash if it's pointing to a folder
    if prefix and not prefix.endswith('/'):
        prefix += '/'

    directories = []
    paginator = s3_client.get_paginator('list_objects_v2')

    # Delimiter='/' tells S3 to group keys by the next slash level
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
        # 'CommonPrefixes' contains the simulated folder names
        if 'CommonPrefixes' in page:
            for item in page['CommonPrefixes']:
                directories.append(item['Prefix'])

    return directories


def read_progress_file(progress_file):
    """
    Reads the progress file and returns a set of completed downloads.
    """
    completed_downloads = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            for line in f:
                completed_downloads.add(line.strip().split('/')[-1])  # Store only the file name, not the full path
    return completed_downloads


def check_downloaded_files(local_path:str, max_files:int) -> int:
    """
    Checks the number of files in the local download directory
    and returns the number that can be safely downloaded without
    exceeding the max_files limit.
    Returns -1 if there is no limit on the number of files.
    """
    if max_files is None:
        return -1 # No limit on the number of files

    all_files = list(Path(local_path).rglob('*'))
    if len(all_files) >= max_files:
        print(f"Warning: The number of files in {local_path} meets or "+\
            f"exceeds the maximum limit of {max_files}.")
        return 0
    else:
        return max_files - len(all_files)


def setup_local_directory(local_path):
    """
    Ensures the local download directory exists.
    """
    if not os.path.exists(local_path):
        os.makedirs(local_path, exist_ok=True)
    print(f"Local download directory is set up at: {local_path}")


def populate_download_queue(s3_client, bucket, sub_folders, completed_files, args):
    """
    Populates the download queue with files to be downloaded from S3.
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    download_queue = set()
    for sub_folder in sub_folders:
        # List files in the S3 subfolder
        for page in paginator.paginate(Bucket=bucket, Prefix=sub_folder):
            if 'Contents' in page:
                for obj in page['Contents']:
                    object_key = obj['Key']
                    # Skip if it's a folder (S3 folders are represented as keys ending with '/')
                    if object_key.endswith('/'):
                        continue
                    if object_key in completed_files:
                        print(f"Skipping already downloaded file: {object_key}")
                        continue                    
                    if args.file_prefix and not object_key.startswith(args.file_prefix):
                        continue  # Skip files that don't match the specified prefix
                    if args.file_suffix and not object_key.endswith(args.file_suffix):
                        continue  # Skip files that don't match the specified suffix
                    download_queue.add(object_key)
    return download_queue

"""
download_s3.py
This script provides functionality to download files from an S3 bucket, including support 
for large files using multipart downloads. It allows filtering of files based on prefixes and suffixes,
and maintains a progress file to avoid re-downloading files that have already been completed.
"""


def main():
    parser = AP(description="Download files from S3 bucket")
    parser.add_argument('--bucket', type=str, required=True, help='S3 bucket name, e.g. 1000g-ont')
    parser.add_argument('--prefix', type=str, default='', help='Prefix to filter objects in the bucket, e.g. PROCESSED_DATA/ALIGNED_TO_HG38/MODKIT/')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region of the S3 bucket')
    parser.add_argument('--local-path', type=str, default='./downloaded_data', help='Local path to save downloaded files')
    parser.add_argument('--file-prefix', type=str, help='Optional file prefix to specific downloads')
    parser.add_argument('--file-suffix', type=str, help='Optional file suffix to specific downloads')
    parser.add_argument('--completed-file', type=str, default='completed.txt', help='List of completed downloads to avoid re-downloading (default "completed.txt")')
    parser.add_argument('--pending-file', type=str, default='pending.txt', help='List of pending downloads to track progress (default "pending.txt")')
    parser.add_argument('--max-files', type=int, default=None, help='Maximum number of files to retain in download folder (default: all)')
    args = parser.parse_args()

    # --- Example Usage ---
    #bucket = '1000genomes'
    #bucket = "1000g-ont"
    #region_name = 'us-east-1'
    #pfx = "PROCESSED_DATA/ALIGNED_TO_HG38/MODKIT/"

    local_base_path = args.local_path
    setup_local_directory(local_base_path)

    # check if files already exist in the local directory
    completed_files = read_progress_file(args.completed_file)
    # remove any local files that are downloaded and not in the progress file
    for file in os.listdir(local_base_path):
        if os.path.isfile(os.path.join(local_base_path, file)) and file not in completed_files \
             and file != args.completed_file and file != args.pending_file:
             print(f"Removing local file not marked as completed: {file}")
             #os.remove(os.path.join(local_base_path, file))
    
    # 1. Initialize an unauthenticated S3 client
    s3_client = boto3.client('s3', region_name=args.region,
        config=Config(signature_version=UNSIGNED))

    # 1. Get folders at the root level of the bucket
    #root_folders = list_s3_directories(bucket, region_name=region_name)
    #print("Root Folders:", root_folders)

    # 2. Get subfolders inside a specific folder
    sub_folders = list_s3_directories(s3_client, args.bucket,
        prefix=args.prefix, region_name=args.region)
    #print(f"Total subfolders inside {args.prefix}:", len(sub_folders))

    # 3. identify download files from each subfolder
    print(f'Populating download queue from bucket: {args.bucket}, prefix: {args.prefix}')
    if args.pending_file and os.path.exists(args.pending_file):
        print(f"Reading pending downloads from {args.pending_file}")
        with open(args.pending_file, 'r') as pf:
            download_queue = set(line.strip() for line in pf if line.strip())
    else:
        download_queue = populate_download_queue(s3_client, args.bucket, 
            sub_folders, completed_files, args)
        with open(args.pending_file, 'w') as pf:
            for object_key in sorted(download_queue):
                pf.write(f"{object_key}\n")

    download_queue = download_queue - completed_files  # Remove already completed files from the queue
    print(f'Total files to download: {len(download_queue)}')
    for object_key in download_queue:
        # Determine the local download path for the file
        local_file_path = Path(local_base_path)/object_key.split('/')[-1]
        
        # Check if we can download more files based on max_files limit
        remaining_files = check_downloaded_files(local_base_path, args.max_files)
        if remaining_files == 0:
            print(f"Reached maximum file limit of {args.max_files}. Stopping further downloads.")
            break
        
        # Download the file
        download_large_file(s3_client, args.bucket, object_key, local_file_path)
        if local_file_path.exists():
            with open(args.completed_file, 'a') as pf:
                pf.write(f"{object_key}\n")    


if __name__ == '__main__':
    main()
