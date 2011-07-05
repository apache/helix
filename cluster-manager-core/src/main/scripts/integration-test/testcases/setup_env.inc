# set up env, figure out the path
#ROOT_DIR=../../../
export PATH_PREFIX_DIR=../
export SCRIPT_DIR=$PATH_PREFIX_DIR/script/
export VAR_DIR=$PATH_PREFIX_DIR/var
export DATA_DIR=$PATH_PREFIX_DIR/data

export SCRIPT_DIR=../script
export CONFIG_DIR=integration-test/config
#export VAR_DIR=../var

DEFAULT_TEST_NAME=`basename $0`

if [ -z "${TEST_NAME}" -o "${TEST_NAME}" = "-bash" ] ; then
  TEST_NAME=${DEFAULT_TEST_NAME}
fi

export TEST_NAME
export VAR_WORK_DIR=../var/work
export VAR_WORK_DIR_FROM_ROOT=intergration-test/var/work

if [ -z "${TEST_NAME}" ] ; then
  export LOG_DIR=../var/log
  export WORK_DIR=../var/work
  export WORK_DIR_FROM_ROOT=integration-test/var/work
  export LOG_DIR_FROM_ROOT=integration-test/var/log
else
  export LOG_DIR=../var/log/${TEST_NAME}
  export WORK_DIR=../var/work/${TEST_NAME}
  export WORK_DIR_FROM_ROOT=integration-test/var/work/${TEST_NAME}
  export LOG_DIR_FROM_ROOT=integration-test/var/log/${TEST_NAME}
fi
#export DATA_DIR=../data
export VIEW_ROOT=`cd ../../; echo $PWD`
# solaris tail
export TAIL_PATH=/usr/xpg4/bin/tail
if [ ! -f $TAIL_PATH ]; then
  export TAIL_PATH=tail
fi
# over all stats
all_stat=0
