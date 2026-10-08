# methyl_1000g_ont
CpG methylation study of 1000g-ont data


## Files

bedmethyl_to_parquet.py - convert 18 column modkit generated .bed.gz files to parquet format with multiprocessing (one file per process)
download_s3.py - multithreaded (15 thread) download of files from s3 bucket. Allows a limit placed on the number of files allowed in the download folder to prevent running out of storage


## License

All code has been created with the assistance of Google Gemini. It is hereby shared under the GPLv2.0 only where it is otherwise not already considered public domain, and may be used and shared without fear or prejudice.



