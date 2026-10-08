#!/usr/bin/env python3

import concurrent.futures
from pathlib import Path
import duckdb
from argparse import ArgumentParser as AP

# Standard bedMethyl BED9+6 columns layout
BEDMETHYL_COLUMNS = [
    "chrom", "chromStart", "chromEnd", "name", "score", "strand",
    "thickStart", "thickEnd", "itemRgb", "coverage", 
    "percentage_modified", "n_modified", "n_unmodified", "n_other", "n_del"
]

def convert_single_file(file_path: Path, output_dir: Path) -> str:
    """Converts a single bedMethyl file into a compressed Parquet partition."""
    # Derive an output filename (e.g., sample_01.parquet)
    out_path = output_dir / f"{file_path.stem}.parquet"

    # Map the exact 18-column modkit pileup schema
    modkit_columns = {
        "chrom": "VARCHAR",            # 1. Chromosome
        "chromStart": "BIGINT",        # 2. 0-based start position
        "chromEnd": "BIGINT",          # 3. End position
        "name": "VARCHAR",             # 4. Modification code / motif
        "score": "INTEGER",            # 5. Scaled score (0-1000)
        "strand": "VARCHAR",           # 6. Strand (+ / -)
        "thickStart": "BIGINT",        # 7. Equal to chromStart
        "thickEnd": "BIGINT",          # 8. Equal to chromEnd
        "itemRgb": "VARCHAR",          # 9. RGB color 
        "coverage": "BIGINT",          # 10. Total valid coverage count
        "percentage": "DOUBLE",        # 11. Percentage of modified bases
        "N_modified": "BIGINT",        # 12. Count of modified base calls
        "N_canonical": "BIGINT",       # 13. Count of canonical base calls
        "N_other_mod": "BIGINT",       # 14. Count of other modifications found here
        "N_delete": "BIGINT",          # 15. Count of deletion alignments
        "N_fail": "BIGINT",            # 16. Count of calls failing quality thresholds
        "N_diff": "BIGINT",            # 17. Count of mismatch/substitution alleles
        "N_nocall": "BIGINT"           # 18. Count of reads with no call made
    }
    
    # Open an independent ephemeral connection per worker thread
    with duckdb.connect() as con:
        query = f"""
            COPY (
                SELECT * FROM read_csv('{file_path}', 
                    sep='\t', 
                    header=False, 
                    comment='#',
                    compression='gzip',
                    quote='',
                    columns={str(modkit_columns)}
                )
            ) TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """
        con.execute(query)
    return f"Done: {file_path.name} -> {out_path.name}"

def batch_process_pipeline(input_folder: str, output_folder: str, max_workers: int = 16):
    in_dir = Path(input_folder)
    out_dir = Path(output_folder)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Locate all target files (.bedmethyl, .bed, or .tsv formats)
    files_to_process = list(in_dir.glob("*.bed.gz")) + list(in_dir.glob("*.bedmethyl.gz")) + list(in_dir.glob("*.tsv.gz"))
    print(f"Found {len(files_to_process)} target files. Starting parallel compilation...")

    # Parallel submission pool maximizing system storage architecture
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(convert_single_file, f, out_dir): f for f in files_to_process}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                result = future.result()
                if i % 50 == 0 or i == len(files_to_process):
                    print(f"[{i}/{len(files_to_process)}] Completed processing threshold mapping.")
            except Exception as exc:
                print(f"File {futures[future].name} generated an exception: {exc}")


def main():
    parser = AP(description="Batch convert bedMethyl files to Parquet format.")
    parser.add_argument("--input_folder", required=True, help="Directory containing raw bedMethyl files.")
    parser.add_argument("--output_folder", required=True, help="Directory to store converted Parquet files.")
    parser.add_argument("--max_workers", type=int, default=12, help="Max parallel threads for conversion.")
    
    args = parser.parse_args()
    
    batch_process_pipeline(args.input_folder, args.output_folder, args.max_workers)


if __name__ == "__main__":
    main()

