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
 * @author kgopalak
 */
package org.apache.helix;
