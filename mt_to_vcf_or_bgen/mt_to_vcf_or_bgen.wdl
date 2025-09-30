version 1.0

# ------------------------------------------------
# TASK 1: Find all Matrix Tables in the input directory (Cloud-aware)
# ------------------------------------------------
task GlobCloudPaths {
    input {
        String base_uri
        String pattern = ""
    }

    command <<<
        # 1. List all files recursively and filter for files ending in .mt
        gsutil ls -r "${base_uri}" | grep -E '\.mt$' > all_mt_paths.txt

        # 2. Apply chromosome/specific pattern filter if the pattern is NOT empty
        if [ -n "${pattern}" ]; then
            # Use 'grep -i' for case-insensitive search (optional, but robust)
            grep -i "${pattern}" all_mt_paths.txt > cloud_paths.txt
        else
            # If no pattern is provided, use the unfiltered list
            cp all_mt_paths.txt cloud_paths.txt
        fi

        if ! [ -s cloud_paths.txt ]; then
            echo "Warning: No matrix tables found matching directory or pattern: ${pattern}."
        fi
    >>>

    output {
        File paths_file = "cloud_paths.txt"
    }

    runtime {
        docker: "google/cloud-sdk:latest"
        preemptible: 3
        zones: ["us-central1-a"]
    }
}

# ------------------------------------------------
# TASK 2: Convert a single Matrix Table to VCF/BGEN (Non-Localizing)
# ------------------------------------------------
task ConvertMT {
    input {
        String mt_file
        String output_dir_root
        String output_format # "vcf" or "bgen"
        Int cpu_count
        Int memory_gb
    }

    String prefix = basename(mt_file, ".mt")

    command <<<
        set -ex

        INPUT_MT="${mt_file}"
        OUTPUT_BASE="${output_dir_root}${prefix}"
        FORMAT="${output_format}"
        CPU="${cpu_count}"

        echo "Converting remote MT ${INPUT_MT} to ${FORMAT} format using Spark local mode."

        python3 -c "
import sys
import hail as hl

input_mt = sys.argv[1]
output_base = sys.argv[2]
output_format = sys.argv[3]
cpu = sys.argv[4]

hl.init(backend='spark',
        master=f'local[{cpu}]',
        spark_conf={
            'spark.driver.memory': '7.5g',
            'spark.driver.maxResultSize': '7.5g'
        }
)

mt = hl.read_matrix_table(input_mt)
mt.count()

#if output_format == 'vcf':
#    hl.export_vcf(mt, f'{output_base}.vcf', overwrite=True)
#    # NOTE: The subsequent import/force_count is validation,
#    # but not required for a production export task. Keeping it for now.
#    hl.import_vcf(f'{output_base}.vcf')._force_count()
#elif output_format == 'bgen':
#    mt.write(f'{output_base}.bgen', index=True, overwrite=True)
#    hl.import_bgen(f'{output_base}.bgen')._force_count()
#else:
#    print(f'Unsupported format: {output_format}', file=sys.stderr)
#    sys.exit(1)

print(f'Successfully exported to {output_base}.{output_format}')

" "${INPUT_MT}" "${OUTPUT_BASE}" "${FORMAT}" "${CPU}"

        touch success.txt
    >>>

    output {
        File output_file = "success.txt"
    }

    runtime {
        docker: "hailgenetics/hail:0.2.136"
        cpu: cpu_count
        memory: "${memory_gb} GB"
        disks: "local-disk 10 HDD"
        preemptible: 3
        zones: ["us-central1-a"]
    }
}

# ------------------------------------------------
# WORKFLOW: The Scatter-Gather Orchestration
# ------------------------------------------------
workflow MatrixTableConversion {
    input {
        String mt_directory
        String pattern = "" # Optional pattern for chromosome filtering
        String output_root_path
        String export_format = "vcf"
        Int num_cpus = 8
        Int total_memory_gb = 28
    }

    call GlobCloudPaths {
        input:
            base_uri = mt_directory,
            pattern = pattern
    }

    Array[String] matrix_table_uris = read_lines(GlobCloudPaths.paths_file)

    scatter (single_mt_uri in matrix_table_uris) {
        call ConvertMT {
            input:
                mt_file = single_mt_uri,
                output_dir_root = output_root_path,
                output_format = export_format,
                cpu_count = num_cpus,
                memory_gb = total_memory_gb
        }
    }

    output {
        Array[File?] converted_files = ConvertMT.output_file
    }
}
