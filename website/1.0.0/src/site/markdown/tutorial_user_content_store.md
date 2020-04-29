<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<head>
  <title>Tutorial - User Defined Content Store for Tasks</title>
</head>

## [Helix Tutorial](./Tutorial.html): User Defined Content Store for Tasks

The purpose of user defined content store is to provide an easy use feature for some task dedicated meta temporary store.
In this chapter, we\'ll learn how to implement and use content store in the user defined tasks.

### Content Store Implementation

Extends abstract class UserContentStore.
    
    private static class ContentStoreTask extends UserContentStore implements Task {
      @Override public TaskResult run() {
        ...
      }
      @Override public void cancel() {
        ...
      }
    }
    
The default methods support 3 types of scopes:
1. WORKFLOW: Define the content store in workflow level
2. JOB: Define the content store in job level
3. TASK: Define the content store in task level

### Content Store Usage

Access content store in Task.run() method.

      private static class ContentStoreTask extends UserContentStore implements Task {
        @Override public TaskResult run() {
          // put values into the store
          putUserContent("ContentTest", "Value1", Scope.JOB);
          putUserContent("ContentTest", "Value2", Scope.WORKFLOW);
          putUserContent("ContentTest", "Value3", Scope.TASK);
          
          // get the values with the same key in the different scopes
          if (!getUserContent("ContentTest", Scope.JOB).equals("Value1") ||
              !getUserContent("ContentTest", Scope.WORKFLOW).equals("Value2") ||
              !getUserContent("ContentTest", Scope.TASK).equals("Value3")) {
            return new TaskResult(TaskResult.Status.FAILED, null);
          }
          
          return new TaskResult(TaskResult.Status.COMPLETED, null);
        }
      }
