#!/bin/bash
# Download all specified files from S3 and merged them to a single text file
#
# Usage:
# s3_sync_mnt __LOCAL_DIR_BASE__ __S3_DIR_KEY__
#
# e.g.,
# s3_sync_mnt "/mnt/s3_sync/" "smartad-dmp/warehouse/ml/exp_libsvm/type=demogra2/dt=2017-03-03"
#  => /mnt/s3_sync/smartad-dmp/warehouse/ml/exp_libsvm/type=demogra2/dt=2017-03-03/

set -eu

if [ "$#" -ne 2 ]; then
    echo "[Usage] fetch_table_data.sh [Output_dir] [s3_bucket]"
    exit 1
fi

dest="$1"
key=$(dirname "$2/x")

mkdir -p $dest

# download parallelly
aws s3 ls "s3://$key/" | awk '{print $NF}' | \
    parallel --retries 2 aws s3 cp --quiet s3://$key/{} "$dest/{}"
# aws s3 sync "s3://$key" "$mnthome/$key"

mergedf="$(cd $dest && pwd)/merged.data"

if [ -f "$mergedf" ]; then
    rm $mergedf
fi

function cat_gz
{
    f="$1"
    if [[ "$f" == *gz ]]; then
        cat $f | gzip -dc
    else
        cat $f
    fi
}

export -f cat_gz

find "$dest" -type f ! -name "merged.data" -exec bash -c 'cat_gz "$0"' {} \; | egrep -v "^\\\N$" >> $mergedf

du -h $mergedf

find "$dest" -type f ! -name "merged.data" -exec rm {} \;
