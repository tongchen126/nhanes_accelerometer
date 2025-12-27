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

case $1 in
       download)
              download "$2" "$3"
       ;;
       unzip_combine)
              unzip_and_combine "$2" "$3"
       ;;
       *)
              echo "Usage ${0} download <URL> <DIR>"
              echo "Usage ${0} unzip_combine <PATH_TO_NHANES_DATA> <PATH_TO_SAVE_CSV>"
              exit 1
       ;;
esac

