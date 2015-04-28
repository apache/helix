package org.apache.helix.participant.statemachine;

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

import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;

/**
 * Finds the method in stateModel to generate
 */
public class StateModelParser {

  public Method getMethodForTransition(Class<? extends TransitionHandler> clazz, String fromState,
      String toState, Class<?>[] paramTypes) {
    Method method = getMethodForTransitionUsingAnnotation(clazz, fromState, toState, paramTypes);
    if (method == null) {
      method = getMethodForTransitionByConvention(clazz, fromState, toState, paramTypes);
    }
    return method;
  }

  /**
   * This class uses the method naming convention "onBecome" + toState + "From"
   * + fromState;
   * @param clazz
   * @param fromState
   * @param toState
   * @param paramTypes
   * @return Method if found else null
   */
  public Method getMethodForTransitionByConvention(Class<? extends TransitionHandler> clazz,
      String fromState, String toState, Class<?>[] paramTypes) {
    Method methodToInvoke = null;
    String methodName = "onBecome" + toState + "From" + fromState;
    if (fromState.equals("*")) {
      methodName = "onBecome" + toState;
    }

    Method[] methods = clazz.getMethods();
    for (Method method : methods) {
      if (method.getName().equalsIgnoreCase(methodName)) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 2 && parameterTypes[0].equals(Message.class)
            && parameterTypes[1].equals(NotificationContext.class)) {
          methodToInvoke = method;
          break;
        }
      }
    }
    return methodToInvoke;

  }

  /**
   * This method uses annotations on the StateModel class. Use StateModelInfo
   * annotation to specify valid states and initial value use Transition to
   * specify "to" and "from" state
   * @param clazz
   *          , class which extends StateModel
   * @param fromState
   * @param toState
   * @param paramTypes
   * @return
   */
  public Method getMethodForTransitionUsingAnnotation(Class<? extends TransitionHandler> clazz,
      String fromState, String toState, Class<?>[] paramTypes) {
    StateModelInfo stateModelInfo = clazz.getAnnotation(StateModelInfo.class);
    Method methodToInvoke = null;
    if (stateModelInfo != null) {
      Method[] methods = clazz.getMethods();
      if (methods != null) {
        for (Method method : methods) {
          Transition annotation = method.getAnnotation(Transition.class);
          if (annotation != null) {
            boolean matchesFrom =
                "*".equals(annotation.from()) || annotation.from().equalsIgnoreCase(fromState);
            boolean matchesTo =
                "*".equals(annotation.to()) || annotation.to().equalsIgnoreCase(toState);
            boolean matchesParamTypes = Arrays.equals(paramTypes, method.getParameterTypes());
            if (matchesFrom && matchesTo && matchesParamTypes) {
              methodToInvoke = method;
              break;
            }
          }
        }
      }
    }

    return methodToInvoke;
  }

  /**
   * Get the initial state for the state model
   * @param clazz
   * @return
   */
  public String getInitialState(Class<? extends TransitionHandler> clazz) {
    StateModelInfo stateModelInfo = clazz.getAnnotation(StateModelInfo.class);
    if (stateModelInfo != null) {
      return stateModelInfo.initialState();
    } else {
      return TransitionHandler.DEFAULT_INITIAL_STATE;
    }
  }

}
