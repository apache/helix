package org.apache.helix.metaclient.puppy;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.metaclient.api.MetaClientInterface;
import java.util.HashMap;


/**
 * AbstractPuppy object contains interfaces to implement puppy and main logics to manage puppy life cycle
 */
public abstract class AbstractPuppy implements Runnable {

  protected MetaClientInterface<String> _metaclient;
  protected PuppySpec _puppySpec;
  public final HashMap<String, Integer> _eventChangeCounterMap;
  public int _unhandledErrorCounter;
  protected final String _parentPath ;

  public AbstractPuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec, String parentPath) {
    _metaclient = metaclient;
    _puppySpec = puppySpec;
    _eventChangeCounterMap = new HashMap<>();
    _parentPath = parentPath;
  }

  /**
   * Implements puppy's main logic. Puppy needs to implement its chaos logic, recovery logic based on
   * errorRate, recoverDelay. For OneOff puppy, it will bark once with execDelay in spec, and for
   * Repeat puppy, it will bark forever, with execDelay between 2 barks
   */
  protected abstract void bark() throws Exception;

  /**
   * Implements puppy's final cleanup logic - it will be called only once right before the puppy terminates.
   * Before the puppy terminates, it needs to recover from all chaos it created.
   */
  protected abstract void cleanup();

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(getPuppySpec().getExecDelay().getNextDelay());
          bark();
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          incrementUnhandledErrorCounter();
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      cleanup();
    }
  }

  public PuppySpec getPuppySpec() {
    return _puppySpec;
  }

  public int getUnhandledErrorCounter() {
    return _unhandledErrorCounter;
  }

  private void incrementUnhandledErrorCounter() {
    _unhandledErrorCounter++;
  }
}



