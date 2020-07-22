set -e
PROJ="ece_454_assignment_3"
DIR=~/"${PROJ}"
SCRIPTS_DIR="arshans_scripts"
SCRIPTS_PATH="${DIR}/${SCRIPTS_DIR}"

cd $SCRIPTS_PATH
./setup_remote.sh
cd $DIR
watchman -j < ./watchman_triggers/java_files.json
watchman -j < ./watchman_triggers/thrift_files.json
watchman -j < ./watchman_triggers/scala_files.json
watchman -j < ./watchman_triggers/bash_files.json
