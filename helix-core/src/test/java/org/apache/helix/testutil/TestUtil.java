package org.apache.helix.testutil;

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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;

public class TestUtil {
  static Logger logger = Logger.getLogger(TestUtil.class);

  public static boolean isTestNGAnnotated(Class<? extends Object> clazz, String methodName) {
    final String annotationsPkgName = "org.testng.annotations";

    // Check if the class itself is annotated.
    Annotation[] classAnnotations = clazz.getAnnotations();
    for (Annotation a : classAnnotations) {
      if (a.annotationType().getPackage().getName().equals(annotationsPkgName)) {
        return true;
      }
    }

    // Check if given method is annotated.
    Method[] methods = clazz.getMethods();
    for (Method m : methods) {
      if (!m.getName().equals(methodName)) {
        continue;
      }
      Annotation[] methodAnnotations = m.getAnnotations();
      for (Annotation a : methodAnnotations) {
        if (a.annotationType().getPackage().getName().equals(annotationsPkgName)) {
          return true;
        }
      }
    }

    return false;
  }

  public static String getTestName() {
    try {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

      // The first 2 elements in the stack are getStackTrace and this method itself, so ignore them.
      for (int i = 2; i < stackTrace.length; i++) {
        Class<? extends Object> clazz = Class.forName(stackTrace[i].getClassName());
        if (isTestNGAnnotated(clazz, stackTrace[i].getMethodName())) {
          String testName = String.format("%s_%s", clazz.getSimpleName(), stackTrace[i].getMethodName());
          logger.debug("Detected " + testName + " as the test name");
          return testName;
        }
      }
    }
    catch (ClassNotFoundException e) {
      throw new RuntimeException("Error while trying to guess test name.", e);
    }

    // No TestNG annotated classes in the stack trace.
    throw new RuntimeException("Unable to guess test name. No TestNG annotated classes or methods in stack trace.");
  }
}
