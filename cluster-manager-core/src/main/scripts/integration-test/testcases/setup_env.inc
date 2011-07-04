# set up env, figure out the path
ROOT_DIR=../../../
PATH_PREFIX_DIR=../
SCRIPT_DIR=$PATH_PREFIX_DIR/script/
CONFIG_DIR=integration-test/config
VAR_DIR=$PATH_PREFIX_DIR/var
LOG_DIR=$VAR_DIR/log
WORK_DIR=$WORK_DIR/work
WORK_DIR_FROM_ROOT=integration-test/var/work
LOG_DIR_FROM_ROOT=integration-test/var/log
DATA_DIR=$PATH_PREFIX_DIR/data
# solaris tail
TAIL_PATH=/usr/xpg4/bin/tail     
if [ ! -f $TAIL_PATH ]; then
  TAIL_PATH=tail
fi
# over all stats
all_stat=0
