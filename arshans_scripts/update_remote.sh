PROJ="ece_454_assignment_3"
DIR=~/"${PROJ}"
SCRIPTS_DIR="arshans_scripts"
SCRIPTS_PATH="${DIR}/${SCRIPTS_DIR}"
cd $SCRIPTS_PATH
source ./common.sh
cd $DIR

function copy_file() {
  file=$1
  remote_path="${REMOTE_HOME}/${PROJ}/${file}"
  dir=$(dirname ${remote_path})
  set -eax
  if test -f "${file}"; then
    echo "Updating: ${file}"
    ssh ${REMOTE} mkdir -p ${dir}
    scp ${file} ${REMOTE}:/${remote_path}
  else
    echo "Deleting: ${file}"
    ssh ${REMOTE} rm ${remote_path}
  fi
}

while read changed_file
do
  echo "Copying: ${changed_file}"
  copy_file ${changed_file}
done < "${1:-/dev/stdin}"
