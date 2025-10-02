version 1.0

# ------------------------------------------------
# TASK 1: Find all Matrix Tables in the input directory
# ------------------------------------------------
task GlobCloudPaths {
    input {
        String base_uri
        String pattern = ""
    }

    command <<<
        # 1. List all files recursively and filter for files ending in .mt
        gsutil ls "~{base_uri}" | grep -E '\.mt/$' > all_mt_paths.txt

        cat all_mt_paths.txt

        # 2. Apply chromosome/specific pattern filter if the pattern is NOT empty
        if [ -n "~{pattern}" ]; then
            grep -i "~{pattern}\.mt" all_mt_paths.txt > cloud_paths.txt
        else
            cp all_mt_paths.txt cloud_paths.txt
        fi

        if ! [ -s cloud_paths.txt ]; then
            echo "Warning: No matrix tables found matching directory or pattern: ~{pattern}."
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
# TASK 2: Convert a single Matrix Table to BGEN (Non-Localizing)
# ------------------------------------------------
task ConvertMT {
    input {
        String mt_file
        String output_dir_root
        Int cpu_count
        Int memory_gb
    }

    String prefix = basename(mt_file, ".mt")

    command <<<
        set -ex

        PLINK_DIR="$PWD/plink_tools"
        mkdir -p ${PLINK_DIR}

        apt-get update && apt-get install -y curl unzip
        curl -O https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20250819.zip
        unzip plink_linux_x86_64_20250819.zip -d ${PLINK_DIR}

        export PATH=${PLINK_DIR}:${PATH}

        INPUT_MT="$(echo ~{mt_file} | sed 's:/*$::')"
        OUTPUT_BASE="~{output_dir_root}~{prefix}"
        CPU="~{cpu_count}"

        echo "Converting remote MT ${INPUT_MT} to bgen format using Spark local mode."

        python3 -c "
import sys
import hail as hl

input_mt = sys.argv[1]
cpu = sys.argv[2]

hl.init(backend='spark',
        master=f'local[{cpu}]',
        spark_conf={
            'spark.driver.memory': '7g',
            'spark.driver.maxResultSize': '7g'
        }
)

mt = hl.read_matrix_table(input_mt)
hl.export_bgen(mt, '/exported_data.bgen')

" "${INPUT_MT}" "${CPU}"

        plink --bgen /exported_data.bgen --sample /exported_data.sample --head --out validation_check

        gsutil cp /exported_data.bgen "${OUTPUT_BASE}.bgen"
        gsutil cp /exported_data.sample "${OUTPUT_BASE}.sample"

        touch success.txt
    >>>


    output {
        File output_file = "success.txt"
    }

    runtime {
        docker: "hailgenetics/hail:0.2.136"
        cpu: cpu_count
        memory: "${memory_gb} GB"
        disks: "local-disk 50 HDD"
        preemptible: 2
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
        Int num_cpus = 16
        Int total_memory_gb = 64
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
                cpu_count = num_cpus,
                memory_gb = total_memory_gb
        }
    }

    output {
        Array[File?] converted_files = ConvertMT.output_file
    }
}
