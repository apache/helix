'''
====  utilities
'''

import sys, os, subprocess
import socket, pdb, re
import urllib, errno
import time, shutil
import tempfile
 
sys_call_debug=False
enable_sys_call_debug=False
debug_enabled=False
host_name_global = (os.popen("/bin/hostname").read()).split(".")[0]

view_root=None
test_name=None
#pdb.set_trace()
this_file_full_path=os.path.abspath(__file__)
# use logical pwd so symlink can be done from root
this_file_dirname=os.path.dirname(this_file_full_path)
#this_file_dirname="PWD" in os.environ and os.environ["PWD"] or os.path.dirname(this_file_full_path)

work_dir=None
log_dir=None
var_dir=None
var_dir_template="%s/integration-test/var"
testcase_dir=None
testcase_dir_template="%s/integration-test/testcases"
cwd_dir=os.getcwd()
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
    global view_root, test_name
    if "VIEW_ROOT" in os.environ: view_root = os.environ["VIEW_ROOT"]
    else: view_root= os.path.abspath("%s/../../" % this_file_dirname)
    if "TEST_NAME" in os.environ:  test_name=os.environ["TEST_NAME"]
    #print("view_root = %s" % view_root)
    os.chdir(view_root)
    os.environ["VIEW_ROOT"]=view_root

def get_view_root(): return view_root

def setup_work_dir():
    global var_dir, work_dir, log_dir, testcase_dir
    var_dir= var_dir_template % (view_root)
    testcase_dir= testcase_dir_template % (view_root)
    import distutils.dir_util
    distutils.dir_util.mkpath(var_dir)
    
    sub_work_dirs=["log","work"]
    #print "test_name=", test_name
    #pdb.set_trace()
    if test_name:
        work_dir=os.path.join(var_dir,"work",test_name)
        log_dir=os.path.join(var_dir,"log",test_name)
        sub_work_dirs.append(os.path.join("log",test_name))
        sub_work_dirs.append(os.path.join("work",test_name))
    else:
        work_dir=os.path.join(var_dir,"work")
        log_dir=os.path.join(var_dir,"log")
    for d in sub_work_dirs:
      try: distutils.dir_util.mkpath(os.path.join(var_dir,d))  
      except: pass

def get_work_dir(): return work_dir
def get_log_dir(): return log_dir
def get_var_dir(): return var_dir
def get_testcase_dir(): return testcase_dir
def get_cwd(): return cwd_dir

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
      sys.exit(1)

def my_warning(s):
    if debug_enabled.debug:
        print ("== " + sys._getframe(1).f_code.co_name + " == " + str(s))
    #logger.warning(s)

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

def process_exist(pid):
    try:
        os.kill(int(pid), 0)
    except OSError, err:
        if err.errno == errno.ESRCH: return False # not running
        elif err.errno == errno.EPERM: return True  # own by others by running
        else: my_error("Unknown error")
    else:
        return True # running

# remote execute related, by default remote execution is off
setup_view_root()
config_dir="%s/integration-test/config" % view_root
remote_config_file="%s/remote_execute_on.cfg" % config_dir
remote_run=False
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
      my_error("Invalid section %s in config file %s" % (section, config))
    if [x for x in ["test_relay, profile_realy, bootstrap_server"] if re.search(x, section)]:
      if not remote_config_parser.has_option(section, "host"):   # set the default host
        remote_config_parser.set(section, "host",host_name_global) 

def parse_config(remote_config_file):
  global remote_run_config
  remote_config_parser = ConfigParser.SafeConfigParser()
  remote_config_parser.read(remote_config_file)
  check_remote_config(remote_config_parser)
  for section in remote_config_parser.sections(): # returns a list of strings of section names
    remote_run_config[section]={}
    for option in remote_config_parser.options(section):
      remote_run_config[section][option]=remote_config_parser.get(section,option)
  #print("remote_config = %s" % remote_config)

if "REMOTE_CONFIG_FILE" in os.environ:   # can be set from env or from a file
  remote_config_file = os.environ["REMOTE_CONFIG_FILE"]
  if not os.path.exists(remote_config_file):  my_error("remote_config_file %s does not exist")
remote_run=os.path.exists(remote_config_file)
if remote_run: remote_config = parse_config(remote_config_file)
 
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

#====End of Utilties============

