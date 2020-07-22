source ./common.sh

function copy_project() {
  scp -r ${DIR} ${REMOTE}:"${REMOTE_HOME}/."
}

copy_project
