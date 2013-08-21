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
/**
 * Provide the classes necessary to create a Helix cluster manager
 * <p>
 * General flow 
 * <blockquote>
 * <pre>
 * manager = HelixManagerFactory.getManagerForROLE(); ROLE can be participant, spectator or a controller<br/>
 * manager.connect();
 * manager.addSOMEListener();
 * After connect the subsequent interactions will be via listener onChange callbacks
 * There will be 3 scenarios for onChange callback, which can be determined using NotificationContext.type
 * INIT -> will be invoked the first time the listener is added
 * CALLBACK -> will be invoked due to datachange in the property value
 * FINALIZE -> will be invoked when listener is removed or session expires
 * 
 * manager.disconnect()
 * </pre>
 * 
 * </blockquote> 
 * 
 * Default implementations available
 * 
 * @see org.apache.helix.participant.HelixStateMachineEngine for participant
 * @see RoutingTableProvider for spectator
 * @see GenericHelixController for controller
 * 
 */
package org.apache.helix;

