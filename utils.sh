#!/bin/bash
TIMEOUT=60
MAX_ROUNDS=5

get_links(){
       lynx -dump -listonly -nonumbers "$1" |grep "tar\.bz2" > "$2"
}

download_from_links(){
       local LINK_PATH=$1
       local DOWNLOAD_DIR=$2
       local FAILED_LINKS=$3
       local round=1
       local success=0
       echo "Downloading files from ${LINK_PATH}"
       rm -f "$FAILED_LINKS"
       cat "$LINK_PATH" | parallel -j 6 --retries 3 --joblog "${FAILED_LINKS}" --bar \
              "curl -fsSL --retry 3 --retry-delay 10 --connect-timeout ${TIMEOUT} --output-dir ${DOWNLOAD_DIR} -O {}"
       
       if [ $? -eq 0 ]; then
              success=1
       else
              while [ $round -le $MAX_ROUNDS ]; do
                     echo "Round $round: failures remaining"
                     sleep 3
                     echo "Retrying..."
                     parallel --retry-failed --retries 1 --joblog "${FAILED_LINKS}" --bar
                     if [ $? -eq 0 ]; then
                            success=1
                            break
                     fi
                     ((round++))
              done
       fi
       if [ $success -eq 1 ]; then
              echo "Download all successfully!"
       else
              echo "Download failures eixst!"
       fi
}

download(){
       local URL=$1
       local LINK_PATH=${2}/logs/links.txt
       local FAILED_LINKS=${2}/logs/log.txt
       local DOWNLOAD_DIR=${2}/data

       mkdir -p "$2"/data
       mkdir -p "$2"/logs
       get_links "$URL" "$LINK_PATH"
       download_from_links "$LINK_PATH" "$DOWNLOAD_DIR" "$FAILED_LINKS"
}

unzip_and_combine(){
       local SOURCE=$1
       local SAVE=$2
       rm -rf $SAVE
       for file in $(ls -d $SOURCE/*); do
              local ID=$(basename "$file" |awk -F '.' '{print $1}')
              local curDIR=${SAVE}/${ID}
              echo "Processing ID: ${ID}"
              mkdir -p ${curDIR}
              tar -xvf "$file" -C "${curDIR}" 1>/dev/null
              exclude_files=$(ls -d ${curDIR}/* | grep -v "GT3.*sensor.*csv")
              echo "Excluded files: $exclude_files"
              for exclude_file in $exclude_files; do
                     rm $exclude_file
              done
              ls -d ${curDIR}/*.csv | xargs awk 'FNR==1 && NR!=1{next;}{print}' > "${curDIR}.csv"
              rm -rf ${curDIR}
       done
}

process_single(){
       local output_dir=$2
       local bz2_file=$3
       local ID=$(basename "$bz2_file" |awk -F '.' '{print $1}')
       local curDIR=$1/$ID
       local rscript_output_dir=${1}/
       local log_file=${1}/LOG/${ID}.log

       echo "Processing ID: ${ID}" > ${log_file} 2>&1
       mkdir -p ${1}/LOG/
       rm -rf ${curDIR}
       mkdir -p ${curDIR}/tmp
       tar -xvf "${bz2_file}" -C "${curDIR}/tmp" 1>/dev/null
       exclude_files=$(ls -d ${curDIR}/tmp/* | grep -v "GT3.*sensor.*csv")
       echo "Excluded files: $exclude_files" >> ${log_file} 2>&1
       for exclude_file in $exclude_files; do
              rm $exclude_file
       done
       ls -d ${curDIR}/tmp/*.csv | xargs awk 'FNR==1 && NR!=1{next;}{print}' > "${curDIR}/${ID}.csv"
       rm -rf ${curDIR}/tmp
       echo "Converting ID: ${ID}" >> ${log_file} 2>&1
       Rscript convert.R ${curDIR} ${rscript_output_dir} >> ${log_file} 2>&1
       if [ $? -eq 0 ]; then
              echo "Rscript finished successfully, ID: ${ID}, " >> ${log_file} 2>&1
              local meta=${rscript_output_dir}/output_${ID}/meta/basic/meta_${ID}.csv.RData
              local ms2=${rscript_output_dir}/output_${ID}/meta/ms2.out/${ID}.csv.RData
              local csv=${rscript_output_dir}/output_${ID}/meta/csv/${ID}.csv.RData.csv
              local output="${output_dir}/${ID}_"
              echo "Python processing, ID: ${ID}, " >> ${log_file} 2>&1
              python utils.py $meta $ms2 $csv $output >> ${log_file} 2>&1
       else
              echo "Rscript error, ID: ${ID}, " >> ${log_file} 2>&1
       fi
}

test_func(){
       echo "$@"
}

process_all(){
       local bz2_dir=$1
       local tmp_dir=$2
       local output_dir=$3
       local num_cpus=$(nproc)
       
       rm -rf $tmp_dir
       rm -rf $output_dir
       mkdir -p ${tmp_dir}/LOG
       mkdir -p $output_dir
       export -f process_single
       echo "Do parallel processing, number of process ${num_cpus}"
       echo "Processing bz2_dir $bz2_dir, tmp dir $tmp_dir, output dir $output_dir"
       parallel --progress --lb -j $num_cpus process_single "${tmp_dir}" "${output_dir}" ::: $(ls -d ${bz2_dir}/* | grep 'bz2$')
}

case $1 in
       download)
              download "$2" "$3"
       ;;
       unzip_combine)
              unzip_and_combine "$2" "$3"
       ;;
       process_all)
              process_all "$2" "$3" "$4"
       ;;
       *)
              echo "Usage ${0} download <URL> <DIR>"
              echo "Usage ${0} unzip_combine <PATH_TO_NHANES_DATA> <PATH_TO_SAVE_CSV> (depracted, use only when unzip and combine is the final aim.)"
              echo "Usage ${0} process_all <PATH_TO_NHANES_BZ2_DIR> <PATH_USED_AS_TMP> <PATH_TO_SAVE>. PATH_USED_AS_TMP is better on a fast SSD."
              exit 1
       ;;
esac
