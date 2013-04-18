#!/usr/bin/env python
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
  Load the dds driver and support file if needed

'''
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2011/6/27 $"

import os, pdb

#pdb.set_trace()

# Global varaibles
meta_data_file=".metadata_infra"
dds_test_infra_tarball="dds_test_infra.tar.gz"

this_file_full_path=os.path.abspath(__file__)
this_file_dirname=os.path.dirname(this_file_full_path)
meta_data_file_full_path=os.path.join(this_file_dirname, meta_data_file)
dds_test_infra_tarball_full_path="%s/../lib/%s" % (this_file_dirname,dds_test_infra_tarball)

need_reload=False
file_change_time = str(os.path.getmtime(dds_test_infra_tarball_full_path))
view_root= os.path.abspath("%s/../../../../../" % this_file_dirname)  # script dir is 5 levels lower
if not os.path.exists(os.path.join(view_root,"integration-test")):
  view_root= os.path.abspath("%s/../../" % this_file_dirname)  # script dir is 5 levels lower
if not os.path.exists(os.path.join(view_root,"integration-test")):
  print "VIEW_ROOT %s is not correct" % view_root
  assert False, "VIEW_ROOT %s is not correct" % view_root

if not os.path.exists(meta_data_file_full_path): need_reload = True
else: 
  last_change_time = open(meta_data_file_full_path).readlines()[0].split("=")[-1]
  if file_change_time != last_change_time:
    need_reload = True
if need_reload:
  open(meta_data_file_full_path,"w").write("change time of %s=%s" % (meta_data_file_full_path, file_change_time))
  # specific to the cm 
  os.system("tar zxf %s > /dev/null" %  dds_test_infra_tarball_full_path)
  #os.system("tar zxf %s " %  dds_test_infra_tarball_full_path)
  integ_java_dir=os.path.join(view_root,"src/test")
  config_dir=os.path.join(this_file_dirname,"../config")
  os.system("cp -rf integ/java %s" % integ_java_dir)
  os.system("rm -rf integ")
  os.system("cp script/* %s" % this_file_dirname)
  os.system("rm -rf script")
  os.system("cp config/* %s" % config_dir)
  os.system("rm -rf config")

os.environ["VIEW_ROOT"]=view_root
execfile(os.path.join(this_file_dirname,"dds_driver.py"))

