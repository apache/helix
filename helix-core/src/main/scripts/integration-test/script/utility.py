#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

'''
====  utilities
'''

import sys, os, subprocess
import socket, pdb, re
import urllib, errno
import time, shutil
import tempfile
import random
import socket
 
sys_call_debug=False
enable_sys_call_debug=False
debug_enabled=False
#host_name_global = (os.popen("/bin/hostname").read()).split("\n")[0]
host_name_global = socket.gethostbyaddr(socket.gethostbyname(socket.gethostname()))[0]

view_root=None
test_name=None
#pdb.set_trace()
this_file_full_path=os.path.abspath(__file__)
# use logical pwd so symlink can be done from root
this_file_dirname=os.path.dirname(this_file_full_path)
this_file_name=os.path.basename(this_file_full_path)
#this_file_dirname="PWD" in os.environ and os.environ["PWD"] or os.path.dirname(this_file_full_path)

work_dir=None
log_dir=None
var_dir=None
var_dir_template="%s/integration-test/var"
testcase_dir=None
testcase_dir_template="%s/integration-test/testcases"
cwd_dir=os.getcwd()
import getpass
username=getpass.getuser()
# used to run cmd, can combine multiple command
components=[
     "test_relay"
    ,"test_bootstrap_producer"
    ,"bootstrap_server"
    ,"bootstrap_consumer"
    ,"profile_relay"
    ,"profile_consumer"
]

def dbg_print(in_str):
    #import pdb
    #pdb.set_trace()
    if debug_enabled:
        print ("== " + sys._getframe(1).f_code.co_name + " == " + str(in_str))

def sys_pipe_call(cmd):
    dbg_print("%s:%s" % (os.getcwd(),cmd))
    if sys_call_debug:
        print("cmd = %s " % cmd)
        if re.search("svn (log|info)",cmd): return os.popen(cmd).read()
        return ""
    return os.popen(cmd).read()               

def get_this_file_dirname(): return this_file_dirname
def get_this_file_name(): return this_file_name
#handle the json import 
if sys.version_info[0]==2 and sys.version_info[1]<6:
  try:
    import simplejson as json
  except: 
    out=sys_pipe_call(os.path.join(get_this_file_dirname(),"install_python_packages.sh"))
    #print("install json = %s " % out)
    import simplejson as json
else:
  import json

# functions
def setup_view_root():
    global view_root
    if "VIEW_ROOT" in os.environ: view_root = os.environ["VIEW_ROOT"]
    else: view_root= os.path.abspath("%s/../../" % this_file_dirname)
    #print("view_root = %s" % view_root)
    #print("test_name=%s" % test_name)
    os.chdir(view_root)
    os.environ["VIEW_ROOT"]=view_root

def get_view_root(): return view_root

def setup_work_dir():
    global var_dir, work_dir, log_dir, test_name
    var_dir= var_dir_template % (view_root)
    import distutils.dir_util
    distutils.dir_util.mkpath(var_dir, verbose=1)

    if "TEST_NAME" in os.environ: test_name=os.environ["TEST_NAME"]
    else: assert False, "TEST NAME Not Defined"
    if "WORK_SUB_DIR" in os.environ: work_dir=os.path.join(var_dir,os.environ["WORK_SUB_DIR"],test_name)
    else: assert False, "Work Dir Not Defined"
    if "LOG_SUB_DIR" in os.environ: log_dir=os.path.join(var_dir, os.environ["LOG_SUB_DIR"], test_name)
    else: assert False, "Work Dir Not Defined"
    distutils.dir_util.mkpath(work_dir, verbose=1)  
    distutils.dir_util.mkpath(log_dir, verbose=1)  

def get_test_name(): return test_name
def get_work_dir(): return work_dir
def get_log_dir(): return log_dir
def get_var_dir(): return var_dir
def get_script_dir(): return get_this_file_dirname()
def get_testcase_dir(): return testcase_dir
def get_cwd(): return cwd_dir
def get_username(): return username

def my_exit(ret):
  # close all the file descriptors
  os.close(1)  # stdin
  os.close(2)  # stdout
  os.close(3)  # stderr
  sys.exit(ret)

def file_exists(file):  # test both 
    ''' return the abs path of the file if exists '''
    if os.path.isabs(file): 
      if os.path.exists(file): return file
      else: return None
    tmp_file=os.path.join(view_root, file)
    if os.path.exists(tmp_file): return tmp_file
    tmp_file=os.path.join(cwd_dir,file)
    if os.path.exists(tmp_file): return tmp_file
    return None
  
def set_debug(flag): 
    global debug_enabled
    debug_enabled=flag

def set_sys_call_debug(flag): 
    global enable_sys_call_debug
    enable_sys_call_debug=flag

def sys_call_debug_begin():
    if not enable_sys_call_debug: return
    global sys_call_debug
    sys_call_debug=True
    
def sys_call_debug_end():
    if not enable_sys_call_debug: return
    global sys_call_debug
    sys_call_debug=False

def sys_call(cmd):
    dbg_print("%s:%s" % (os.getcwd(),cmd))
    if sys_call_debug:
        print("cmd = %s " % cmd)
        return
    return os.system(cmd)

def subprocess_call_1(cmd, outfp=None):
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        if outfp:
          p = subprocess.Popen(cmd, shell=True, stdout=outfp, stderr=outfp, close_fds=True)
        else:
          #pdb.set_trace()
          p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        return p
    else:
        print("cmd = %s " % cmd)
        return None

def sys_pipe_call_4(cmd):
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        return p.stdout
    else:
        None

def sys_pipe_call_3(cmd):
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        #p = os.popen(cmd)
        return (p.stdout, p.pid)
    else:
        None

def sys_pipe_call_5(cmd):
    ''' return both stdin, stdout and pid '''
    dbg_print("%s:%s" % (os.getcwd(), cmd))
    if not sys_call_debug:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        dbg_print("subprocess pid = %s" % p.pid)
        #p = os.popen(cmd)
        return (p.stdout, p.stderr, p.pid)
    else:
        None

def sys_pipe_call_21(input, cmd):
    ''' call with input pipe to the cmd '''
    dbg_print("%s:%s:%s" % (os.getcwd(),input, cmd))
    if not sys_call_debug:
      return  subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True).communicate(input)[0]
    else:
      return  ""

def sys_pipe_call_2(input, cmd):
    ''' call with input pipe to the cmd '''
    dbg_print("%s:%s:%s" % (os.getcwd(),input, cmd))
    if not sys_call_debug:
      return  subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True).communicate(input)[0]
    else:
      return  ""

def sys_pipe_call_1(cmd):
    ''' also return the errors '''
    dbg_print("%s:%s" % (os.getcwd(),cmd))
    if not sys_call_debug:
      p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
      return p.stdout.readlines()
    else:
      return  ""

#    dbg_print("%s:%s" % (os.getcwd(),cmd))
#    return os.popen4(cmd)[1].read()

def sys_call_env(cmd):
    cmds=cmd.split()
    dbg_print("cmds= %s " % cmds)
    os.spawnv( os.P_WAIT, cmds[0], cmds[1:])

def whoami():
    return sys._getframe(1).f_code.co_name

def my_error(s):
    if debug_enabled: assert False, "Error: %s" % s
    else: 
      print "Error: %s" % s
      my_exit(1)

def my_warning(s):
    if debug_enabled:
        print ("== " + sys._getframe(1).f_code.co_name + " == " + str(s))
    else: 
      print "WARNING: %s" % s

def enter_func():
    dbg_print ("Entering == " + sys._getframe(1).f_code.co_name + " == ")

def get_time():
    return float("%0.4f" % time.time())   # keep 2 digits

def isOpen(ip,port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((ip, int(port)))
      s.shutdown(2)
      return True
    except:
      return False

def next_available_port(ip,port):
    port_num = int(port)
    while (isOpen(ip, port_num)): port_num +=1
    return port_num

def find_open_port(host, start_port, seq_num):
    ''' find the seq_num th port starting from start_port ''' 
    limit = 100
    start_port_num = int(start_port)
    seq = 0
    for i in range(limit):
      port_num = start_port_num + i
      if isOpen(host, port_num): seq += 1
      if seq == seq_num: return port_num
    return None

def process_exist(pid, host=None):
    if not host:
      try:
	  os.kill(int(pid), 0)
      except OSError, err:
	  if err.errno == errno.ESRCH: return False # not running
	  elif err.errno == errno.EPERM: return True  # own by others by running
	  else: my_error("Unknown error")
      else:
          return True # running
    else:  # remote run
      process_cnt = sys_pipe_call("ssh %s@%s 'ps -ef | grep %s | wc -l" % (username, host, pid)).split("\n")[0]
      return process_cnt > 0

# remote execute related, by default remote execution is off
setup_view_root()
config_dir="%s/integration-test/config" % view_root
remote_config_file="%s/remote_execute_on.cfg" % config_dir
remote_run=False    # this is to indicate use of remote_config
remote_launch=False  # this is to indicate remote ssh recursive call
remote_run_config={}
remote_view_root=None
def get_remote_view_root(): return remote_view_root
def set_remote_view_root(v_root): 
    global remote_view_root
    remote_view_root = v_root
def get_remote_log_dir(): 
    return os.path.join(var_dir_template % remote_view_root, "log")
def get_remote_work_dir(): 
    return os.path.join(var_dir_template % remote_view_root, "work")

import ConfigParser

def check_remote_config(remote_config_parser):
  allowed_options=["host","port","view_root"]
  section_names = remote_config_parser.sections() # returns a list of strings of section names
  for section in section_names:
    if not [x for x in components if re.search(x, section)]:
      my_error("Invalid section %s in config file " % (section))
    if [x for x in ["test_relay, profile_realy, bootstrap_server"] if re.search(x, section)]:
      if not remote_config_parser.has_option(section, "host"):   # set the default host
        remote_config_parser.set(section, "host",host_name_global) 

def parse_config_cfg(remote_config_file):
  remote_config_parser = ConfigParser.SafeConfigParser()
  remote_config_parser.read(remote_config_file)
  #check_remote_config(remote_config_parser)   # do not check for now
  for section in remote_config_parser.sections(): # returns a list of strings of section names
    remote_run_config[section]={}
    for option in remote_config_parser.options(section):
      remote_run_config[section][option]=remote_config_parser.get(section,option)
  return remote_run_config

def parse_config_json(remote_config_file):
  return json.load(open(remote_config_file))

def parse_config(remote_config_file_input):
  global remote_run_config, remote_run
  remote_config_file = file_exists(remote_config_file_input)
  if not remote_config_file: my_error("remote_config_file %s does not existi!!" % remote_config_file_input)
  file_type = os.path.splitext(remote_config_file)[1].lower()
  if file_type not in (".cfg",".json"): my_error("remote_config_file type %s is not .json or .cfg file" % file_type)
  file_type = file_type.lstrip(".")
  remote_run_config = globals()["parse_config_%s" % file_type](remote_config_file)
  remote_run = True

def is_remote_run(): return remote_run
def is_remote_launch(): return remote_launch
def set_remote_launch(): 
  global remote_launch
  remote_launch=True
def get_remote_run_config(): return remote_run_config

if "REMOTE_CONFIG_FILE" in os.environ:   # can be set from env or from a file
  parse_config(os.environ["REMOTE_CONFIG_FILE"])
  remote_launch = True # env will not replicated across, so set env will enable launch
 
# url utilities
def quote_json(in_str):
    ret = re.sub('([{,])(\w+)(:)','\\1"\\2"\\3', in_str)
    dbg_print("ret = %s" % ret)
    return ret

def send_url(url_str):
    dbg_print("url_str = %s" % url_str)
    usock = urllib.urlopen(url_str)
    output = usock.read()
    dbg_print("output = %s" % output)
    usock.close()
    return output

# sqlplus
default_db_port=1521
conn_str_template="%%s/%%s@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%%s)(PORT=%s)))(CONNECT_DATA=(SERVICE_NAME=%%s)))" % default_db_port
sqlplus_cmd="sqlplus"
#sqlplus_cmd="NLS_LANG=_.UTF8 sqlplus"   # handle utf8
sqlplus_heading='''
set echo off
set pages 50000
set long 2000000000
set linesize 5000
column xml format A5000
set colsep ,     
set trimspool on 
set heading off
set headsep off  
set feedback 0
-- set datetime format
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MON-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MON-DD HH24:MI:SS.FF3';
'''
# use milliseconds

def exec_sql_one_row(qry, user, passwd, sid, host):
    return exec_sql(qry, user, passwd, sid, host, True)[0]

def exec_sql_split_results(result_line):
    dbg_print("result_line = %s" % result_line)
    # validate to see if there are errors
    err_pattern = re.compile("ORA-\d+|SP2-\d+")
    is_err=err_pattern.search(result_line)
    if is_err: return [["DBERROR","|".join([r.lstrip() for r in result_line.split("\n") if r != ""])]]
    else: return [[c.strip() for c in r.split(",")] for r in result_line.split("\n") if r != ""]

def exec_sql(qry, user, passwd, sid, host, do_split=False):
    ''' returns an list of results '''
    dbg_print("qry = %s" % (qry)) 
    sqlplus_input="%s \n %s; \n exit \n" % (sqlplus_heading, qry)
    #(user, passwd, sid, host) = tuple(area_conn_info[options.area])
    dbg_print("conn info= %s %s %s %s" % (user, passwd, sid, host))
    sqlplus_call="%s -S %s" % (sqlplus_cmd, conn_str_template % (user, passwd, host, sid))
    os.environ["NLS_LANG"]=".UTF8"  # handle utf8
    ret_str = sys_pipe_call_2(sqlplus_input, sqlplus_call) 
    dbg_print("ret_str = %s" % ret_str)
    # may skip this
    if do_split: return exec_sql_split_results(ret_str)
    else: return ret_str

def parse_db_conf_file(db_config_file, db_src_ids_str=""):
    global db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids
    db_src_info={}
    db_sources=json.load(open(db_config_file))
    uri = db_sources["uri"]
    db_conn_user_id = (uri.split("/")[0]).split(":")[-1]
    db_conn_user_passwd = (uri.split("@")[0]).split("/")[-1]
    db_host= (uri.split("@")[1]).split(":")[0]
    tmp = uri.split("@")[1]
    if tmp.find("/") != -1: db_sid = tmp.split("/")[-1]
    else: db_sid = tmp.split(":")[-1]
    dbg_print("db_conn_user_id = %s, db_conn_user_passwd = %s, db_host = %s, db_sid = %s" % (db_conn_user_id, db_conn_user_passwd, db_host, db_sid))

    schema_registry_dir=os.path.join(get_view_root(),"schemas_registry")
    schema_registry_list=os.listdir(schema_registry_dir)
    schema_registry_list.sort()
    sources={}
    for src in db_sources["sources"]: sources[src["id"]]=src
    if db_src_ids_str: 
      if db_src_ids_str=="all": db_src_ids=sources.keys()
      else: db_src_ids = [int(x) for x in db_src_ids_str.split(",")]
    else: db_src_ids=[]
    for src_id in db_src_ids:
      if src_id not in sources: 
        my_error("source id %s not in config file %s. Available source ids are %s" % (src_id, db_config_file, sources.keys()))
      src_info = sources[src_id]
      src_name = src_info["name"].split(".")[-1]
      db_avro_file_path = os.path.join(schema_registry_dir,[x for x in schema_registry_list if re.search("%s.*avsc" % src_name,x)][-1])
      if not os.path.exists(db_avro_file_path): my_error("Schema file %s does not exist" % db_avro_file_path)
      db_user_id = db_conn_user_id
      src_uri = src_info["uri"]
      if src_uri.find(".") != -1: db_user_id = src_uri.split(".")[0]
      db_src_info[src_id] = {"src_name":src_name,"db_user_id":db_user_id, "db_avro_file_path":db_avro_file_path, "uri":src_uri}
      dbg_print("db_src_info for src_id %s = %s" % (src_id, db_src_info[src_id]))
    return db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids

''' mysql related stuff '''
mysql_cmd="mysql"
def get_mysql_call(dbname, user, passwd, host):
    conn_str = "%s -s " % mysql_cmd
    if dbname: conn_str += "-D%s " % dbname
    if user: conn_str += "-u%s " % user
    if passwd: conn_str += "-p%s " % passwd
    if host: conn_str += "-P%s " % host
    return conn_str

def mysql_exec_sql_one_row(qry, dbname=None, user=None, passwd=None, host=None):
    ret = mysql_exec_sql(qry, dbname, user, passwd, host, True)
    dbg_print("ret = %s" % ret)
    if ret: return ret[0]
    else: return None

def mysql_exec_sql_split_results(result_line):
    dbg_print("result_line = %s" % result_line)
    # validate to see if there are errors
    err_pattern = re.compile("ERROR \d+")
    is_err=err_pattern.search(result_line)
    if is_err: return [["DBERROR","|".join([r.lstrip() for r in result_line.split("\n") if r != ""])]]
    else: return [[c.strip() for c in r.split("\t")] for r in result_line.split("\n") if r != ""]

def mysql_exec_sql(qry, dbname=None, user=None, passwd=None, host=None, do_split=False):
    ''' returns an list of results '''
    dbg_print("qry = %s" % (qry)) 
    mysql_input=" %s; \n exit \n" % (qry)
    dbg_print("conn info= %s %s %s %s" % (dbname, user, passwd, host))
    mysql_call=get_mysql_call(dbname, user, passwd, host)
    dbg_print("mysql_call= %s" % (mysql_call))
    #if not re.search("select",qry): return None # test only, select only
    ret_str = sys_pipe_call_21(mysql_input, mysql_call)   # also returns the error
    dbg_print("ret_str = %s" % ret_str)
    # may skip this
    if do_split: return mysql_exec_sql_split_results(ret_str)
    else: return ret_str

def get_copy_name(input_file_name):
    input_f = os.path.basename(input_file_name)
    input_f_split =  input_f.split(".")
    append_idx = min(len(input_f_split)-2,0)
    input_f_split[append_idx] += time.strftime('_%y%m%d_%H%M%S')
    new_file= os.path.join(work_dir, ".".join(input_f_split))
    return new_file

def save_copy(input_files):
    for i in range(len(input_files)):
      new_file= get_copy_name(input_files[i])
      dbg_print("Copy %s to %s" % (input_files[i], new_file))
      if not remote_run:
        shutil.copy(input_files[i], new_file)
      else:
        remote_run_copy(input_files[i], new_file, i)
      input_files[i] = new_file
    return input_files

def save_copy_one(input_file):
    ''' wrapper for save copy '''
    input_files=[input_file]
    save_copy(input_files)
    return input_files[0]

def db_config_detect_host_nomral_open(db_host, db_port, db_user=None, passwd=None, db_sid=None):
    return isOpen(db_host, db_port)

def db_config_detect_host_oracle_open(db_host, db_port, db_user, passwd, db_sid):
    ret = exec_sql("exit", db_user, passwd, db_sid, db_host, do_split=False)
    return not re.search("ERROR:",ret)

def db_config_detect_host(db_host, db_port=default_db_port, detect_oracle=False, db_user=None, passwd=None, db_sid=None):
    detect_func = detect_oracle and db_config_detect_host_oracle_open or db_config_detect_host_nomral_open
    if detect_func(db_host, db_port, db_user, passwd, db_sid): return (db_host, db_port)  # OK
    possible_hosts = ["localhost"]        # try local host
    found_host = False
    for new_db_host in possible_hosts:
      if not detect_func(new_db_host, db_port, db_user, passwd, db_sid): continue
      found_host = True
      break
    if not found_host: my_error("db server on %s and possible hosts %s port %s is down" % (db_host, possible_hosts, db_port))
    print "Substitue the host %s with %s" % (db_host, new_db_host)
    return (new_db_host, db_port)

def db_config_change(db_relay_config):
    ''' if there is a config file, handle the case that db is on on local host '''
    (db_conn_user_id, db_sid, db_host, db_conn_user_passwd, db_src_info, db_src_ids) = parse_db_conf_file(db_relay_config)
    (new_db_host, new_db_port) = db_config_detect_host(db_host, detect_oracle=True, db_user=db_conn_user_id, passwd=db_conn_user_passwd, db_sid=db_sid)
    if new_db_host == db_host: return db_relay_config
    new_db_config_file = get_copy_name(db_relay_config)
    print "New config file is %s" % (new_db_config_file)
    host_port_re = re.compile("@%s:%s:" % (db_host, default_db_port))
    new_host_port = "@%s:%s:" % (new_db_host, new_db_port)
    new_db_config_f = open(new_db_config_file, "w")
    for line in open(db_relay_config):
      new_db_config_f.write("%s" % host_port_re.sub(new_host_port, line))
    return new_db_config_file

# get a certain field in url response
def http_get_field(url_template, host, port, field_name):
    out = send_url(url_template % (host, port)).split("\n")[1]
    dbg_print("out = %s" % out)
    if re.search("Exception:", out): my_error("Exception getting: %s" % out)
    # work around the invalid json, with out the quote, DDS-379
    out=quote_json(out)
    field_value = json.loads(out)
    return field_value[field_name]

# wait util
# Classes
class RetCode:
  OK=0
  ERROR=1
  TIMEOUT=2
  DIFF=3
  ZERO_SIZE=4

# wait utility
def wait_for_condition_1(cond_func, timeout=60, sleep_interval = 0.1):
  ''' wait for a certain cond. cond could be a function. 
     This cannot be in utility. Because it needs to see the cond function '''
  #dbg_print("cond = %s" % cond)
  if sys_call_debug: return RetCode.OK
  sleep_cnt = 0
  ret = RetCode.TIMEOUT
  while (sleep_cnt * sleep_interval < timeout):
    dbg_print("attempt %s " % sleep_cnt)
    if cond_func():
      dbg_print("success")
      ret = RetCode.OK
      break
    time.sleep(sleep_interval)
    sleep_cnt += 1
  return ret

def wait_for_port(host, port):
  def test_port_not_open():
    return not isOpen(host, port) 
  ret = wait_for_condition_1(test_port_not_open, timeout=20, sleep_interval=2)
  if ret != RetCode.OK:
    print "ERROR: host:port %s%s is in use" % (host, port)
  return ret

# find child pid contains java
# works with linux
def find_java_pid(this_pid):
  cmd = sys_pipe_call("ps -o command --pid %s --noheader" % this_pid).split("\n")[0]
  dbg_print("cmd = %s" % cmd)
  cmd_split = cmd.split()
  if len(cmd_split) ==0: return None
  if re.search("java$", cmd_split[0]): return this_pid
  child_processes = [x for x in sys_pipe_call("ps -o pid --ppid %s --noheader" % this_pid).split("\n") if x!=""]
  for child_process in child_processes:
    dbg_print("child_process = %s" % child_process)
    child_pid = child_process.split()[0]
    java_pid = find_java_pid(child_pid)
    if java_pid: return java_pid
  return None

# pid info
PROCESS_INFO_FILE_NAME="process_info.json"
def set_work_dir(dir):
  global work_dir
  work_dir = dir

def get_process_info_file(dir=None):
  if not dir: dir = get_work_dir()
  return os.path.join(dir,PROCESS_INFO_FILE_NAME)  # need to do this after init

def validate_process_info_file():
  process_info_file = get_process_info_file()
  if os.path.exists(process_info_file): 
    return process_info_file
  else: my_error("Process info file %s for test '%s' does not exist. Please run setup first or give correct test name." % (process_info_file, get_test_name()))

def get_process_info(process_info_file=None):
  if not process_info_file: process_info_file = get_process_info_file()
  if file_exists(process_info_file): 
    try: 
      process_info = json.load(open(process_info_file))
    except ValueError: 
      my_error("file %s does not have a valid json. Please remove it." % process_info_file)
  else: 
    my_warning("process_info_file %s does not exist" % process_info_file)
    process_info = {}
  return process_info

def process_info_get_pid_from_log(log_file):
  log_file_handle = open(log_file)
  for i in range(10):
    line = log_file_handle.readline()
    m = re.search("^([0-9]+)$", line)
    if i==0 and m:
      return m.group(1)
    m = re.search("## java process pid = ([0-9]+)", line)
    if m:
      return m.group(1)
  my_error("cannot find pid in log_file %s" % log_file)

def get_process_info_key(component,id):
  return "%s:%s" % (component, id)

def split_process_info_key(key):
  ''' split into component and id '''
  return tuple(key.split(":"))

def save_process_info(component, id, port, log_file, host=None, admin_port=None, mysql_port=None):    
  # port can be None
  process_info = get_process_info()
  key = get_process_info_key (component, id)
  process_info[key]={}
  process_info[key]["host"] = host !=None and host or host_name_global
  process_info[key]["port"] = port 
  process_info[key]["view_root"] = get_view_root()
  if not re.search("^mysql", component):
   process_info[key]["port_byteman"] = port and int(port) + 1000 or random.randint(16000,17000)
  process_info[key]["pid"] =  process_info_get_pid_from_log(log_file)
  if admin_port: process_info[key]["port_admin"] =  admin_port
  if mysql_port: process_info[key]["port_mysql"] =  mysql_port
  process_info_file = get_process_info_file()
  process_info_fs = open(process_info_file, "w")
  json.dump(process_info, process_info_fs ,sort_keys=True,indent=4)
  process_info_fs.close()

  if key not in process_info: my_error("key %s is not in process_info" % (key, process_info))
  return process_info

def list_process():
  process_info_file = validate_process_info_file()
  print "=== Process info for test '%s': %s ===" % (get_test_name(), process_info_file)
  print "".join(open(process_info_file).readlines())

def get_down_process():
  process_info_file = validate_process_info_file()
  process_info = get_process_info()
  down_process={}
  for key in process_info:
    pid = process_info[key]["pid"]
    dbg_print("checking (%s:%s)" % (key,pid))
    if not process_exist(pid,remote_run and process_info[key]["host"] or None):
      down_process[key] = pid
  return down_process

def check_process_up():
  down_process = get_down_process()
  if down_process:
    for key in down_process:
      print  "(%s:%s) is down" % (key, down_process[key])
    return False
  else:
    return True  # all the processes are up

# == remote run
def get_remote_host(component, id="1", field="host"):
  if not is_remote_run(): return "localhost"
  key = get_process_info_key(component,id)
  if key not in get_remote_run_config():
    my_error("Cannot find remote host for %s in remote_run_config" % (key))
  return get_remote_run_config()[key][field]

def get_remote_view_root(component, id="1"):
  get_remote_host(component,id,"view_root")

def need_remote_run(process_info):
  for k, v in process_info.items():
    if not re.search("^mysql",k):  # fiter out mysql
      if v["host"].split(".")[0] != host_name_global.split(".")[0]:
         return True   # need remote run 
  return False

metabuilder_file=".metabuilder.properties"
def get_bldfwk_dir():
    bldfwk_file = "%s/%s" % (get_view_root(), metabuilder_file)
    bldfwk_dir = None
    if not os.path.exists(bldfwk_file): return None
    for line in open(bldfwk_file):
      m = re.search("(bldshared-[0-9]+)",line)
      if m: 
        bldfwk_dir= m.group(1)
        break
    print "Warning. Cannot find bldshared-dir, run ant -f bootstrap.xml"
    #assert bldfwk_dir, "Cannot find bldshared-dir, run ant -f bootstrap.xml"
    return bldfwk_dir

rsync_path="/usr//bin/rsync"
remote_deploy_cmd_template='''rsync -avz  --exclude=.svn --exclude=var --exclude=test-output --exclude=lucene-indexes --exclude=mmap --exclude=mmappedBuffer --exclude=eventLog --exclude=cp_com_linkedin_events --exclude=dist --rsync-path=%s %s/ %s:%s'''
remote_deploy_bldcmd_template='''rsync -avz  --exclude=.svn --rsync-path=%s %s %s:%s'''
remote_deploy_change_blddir_cmd_template='''ssh %s "sed 's/%s/%s/' %s > %s_tmp; mv -f %s_tmp %s" '''
def do_remote_deploy(reset=False):
    global rsync_path
    if not remote_run:  # check
      my_error("Remote config file is not set. use --remote_config_file or set REMOTE_CONFIG_FILE!")
    bldfwk_dir = get_bldfwk_dir()
    view_root = get_view_root()
    already_copied={}
    for section in remote_run_config:
      remote_host = remote_run_config[section]["host"]
      remote_view_root = remote_run_config[section]["view_root"]
      key = "%s:%s" % (remote_host, remote_view_root)
      if key in already_copied: 
        print "Already copied. Skip: host: %s, view_root: %s" % (remote_host, remote_view_root)
        continue
      else: already_copied[key]=1
      if "rsync_path" in remote_run_config[section]: rsync_path=remote_run_config[section]["rsync_path"]
      remote_view_root_parent = os.path.dirname(remote_view_root)
      if reset: sys_call("ssh %s rm -rf %s" % (remote_host, remote_view_root))
      sys_call("ssh %s mkdir -p %s" % (remote_host, remote_view_root))
      cmd = remote_deploy_cmd_template % (rsync_path, view_root, remote_host, remote_view_root)
      sys_call(cmd) 
      if bldfwk_dir:
        cmd = remote_deploy_bldcmd_template % (rsync_path, os.path.join(os.path.dirname(view_root),bldfwk_dir), remote_host, remote_view_root_parent)
        sys_call(cmd) 
        # replace the metabuilder, TODO, escape the /
        metabuilder_full_path = os.path.join(remote_view_root, metabuilder_file)
        cmd = remote_deploy_change_blddir_cmd_template % (remote_host, view_root.replace("/","\/"), remote_view_root.replace("/","\/"), metabuilder_full_path,  metabuilder_full_path,  metabuilder_full_path,  metabuilder_full_path)
        sys_call(cmd) 
      # copy gradle cache
      gradle_cache_template = "%s/.gradle/cache"
      gradle_cache_dir = gradle_cache_template % os.environ["HOME"] 
      if remote_host.split(".")[0] != host_name_global.split(".")[0] and gradle_cache_dir:
        ret = sys_pipe_call("ssh %s pwd" % (remote_host))
        remote_home = ret.split("\n")[0] 
        ret = sys_call("ssh %s mkdir -p %s " % (remote_host, (gradle_cache_template % remote_home)))
        cmd = "rsync -avz --rsync-path=%s %s/ %s:%s" % (rsync_path, gradle_cache_dir , remote_host, gradle_cache_template % remote_home)
        sys_call(cmd)
    return RetCode.OK

def get_remote_host_viewroot_path():
  ''' returns host, view_root, rsync path for each pair of uniq (host,view_root) '''
  host_viewroot_dict = {}
  for component in remote_run_config:
    remote_host = remote_run_config[component]["host"]
    remote_view_root = remote_run_config[component]["view_root"]
    combined_key = "%s,%s" % (remote_host, remote_view_root)
    if remote_host.split(".")[0] != host_name_global.split(".")[0] and not combined_key in host_viewroot_dict:
      host_viewroot_dict[combined_key] =  "rsync_path" in remote_run_config[component] and remote_run_config[component]["rsync_path"] or rsync_path
  keys = host_viewroot_dict.keys()
  ret = []
  for k in keys: 
    l = k.split(",")
    l.append(host_viewroot_dict[k])
    ret.append(tuple(l))
  return ret

#====End of Utilties============

