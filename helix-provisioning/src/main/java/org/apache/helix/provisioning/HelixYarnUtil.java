package org.apache.helix.provisioning;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.log4j.Logger;

public class HelixYarnUtil {
  private static Logger LOG = Logger.getLogger(HelixYarnUtil.class);

  @SuppressWarnings("unchecked")
  public static <T extends ApplicationSpecFactory> T createInstance(String className) {
    Class<ApplicationSpecFactory> factoryClazz = null;
    {
      try {
        factoryClazz =
            (Class<ApplicationSpecFactory>) Thread.currentThread().getContextClassLoader()
                .loadClass(className);
      } catch (ClassNotFoundException e) {
        try {
          factoryClazz =
              (Class<ApplicationSpecFactory>) ClassLoader.getSystemClassLoader().loadClass(
                  className);
        } catch (ClassNotFoundException e1) {
          try {
            factoryClazz = (Class<ApplicationSpecFactory>) Class.forName(className);
          } catch (ClassNotFoundException e2) {

          }
        }
      }
    }
    System.out.println(System.getProperty("java.class.path"));
    if (factoryClazz == null) {
      LOG.error("Unable to find class:" + className);
    }
    ApplicationSpecFactory factory = null;
    try {
      factory = factoryClazz.newInstance();
    } catch (Exception e) {
      LOG.error("Unable to create instance of class: " + className, e);
    }
    return (T) factory;
  }
}
