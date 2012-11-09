package org.apache.helix.integration;

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

import java.util.Date;

import org.testng.annotations.Test;

/**
 * This is a simple integration test. We will use this until we have framework
 * which helps us write integration tests easily
 *
 */

public class IntegrationTest extends ZkStandAloneCMTestBase
{
  @Test
  public void integrationTest() throws Exception
  {
    System.out.println("START IntegrationTest at " + new Date(System.currentTimeMillis()));
//    Thread.currentThread().join();
    System.out.println("END IntegrationTest at " + new Date(System.currentTimeMillis()));
  }
}
