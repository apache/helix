package org.apache.helix.messaging.handling;

import java.util.List;


public interface MultiTypeMessageHandlerFactory extends MessageHandlerFactory {

  List<String> getMessageTypes();
  
}
