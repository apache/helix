package org.apache.helix.model.builder;

import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.Transition;

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
 * Specify a ConstraintItem based on a message (commonly used for transition constraints)
 */
public class MessageConstraintItemBuilder {
  private ConstraintItemBuilder _builder;

  /**
   * Instantiate the builder
   */
  public MessageConstraintItemBuilder() {
    _builder = new ConstraintItemBuilder();
  }

  /**
   * Set the message type of the constraint
   * @param messageType message type
   * @return MessageConstraintItemBuilder
   */
  public MessageConstraintItemBuilder messageType(MessageType messageType) {
    _builder.addConstraintAttribute(ConstraintAttribute.MESSAGE_TYPE.toString(),
        messageType.toString());
    return this;
  }

  /**
   * Set a participant as part of the constraint scope
   * @param participantId the participant to constrain
   * @return MessageConstraintItemBuilder
   */
  public MessageConstraintItemBuilder participant(ParticipantId participantId) {
    _builder.addConstraintAttribute(ConstraintAttribute.INSTANCE.toString(),
        participantId.stringify());
    return this;
  }

  /**
   * Set a resource as part of the constraint scope
   * @param resourceId the resource to constrain
   * @return MessageConstraintItemBuilder
   */
  public MessageConstraintItemBuilder resource(ResourceId resourceId) {
    _builder
        .addConstraintAttribute(ConstraintAttribute.RESOURCE.toString(), resourceId.stringify());
    return this;
  }

  /**
   * Set the transition to constrain for transition message types
   * @param transition the transition to constrain
   * @return MessageConstraintItemBuilder
   */
  public MessageConstraintItemBuilder transition(Transition transition) {
    // if this is a transition constraint, the message type must be STATE_TRANSITION
    _builder.addConstraintAttribute(ConstraintAttribute.MESSAGE_TYPE.toString(),
        Message.MessageType.STATE_TRANSITION.toString());
    _builder.addConstraintAttribute(ConstraintAttribute.TRANSITION.toString(),
        transition.toString());
    return this;
  }

  /**
   * Set the value of the constraint
   * @param value constraint value
   * @return MessageConstraintItemBuilder
   */
  public MessageConstraintItemBuilder constraintValue(String value) {
    _builder.addConstraintAttribute(ConstraintAttribute.CONSTRAINT_VALUE.toString(), value);
    return this;
  }

  /**
   * Get the ConstraintItem instance that is built
   * @return ConstraintItem
   */
  public ConstraintItem build() {
    return _builder.build();
  }
}
