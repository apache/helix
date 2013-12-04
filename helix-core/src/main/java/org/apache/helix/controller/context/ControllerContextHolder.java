package org.apache.helix.controller.context;

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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.serializer.StringSerializer;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * Wrap a {@link ControllerContext} so that it can be persisted in the backing store
 */
public class ControllerContextHolder extends HelixProperty {
  private enum Fields {
    SERIALIZER_CLASS,
    CONTEXT,
    CONTEXT_CLASS
  }

  private static final Logger LOG = Logger.getLogger(ControllerContextHolder.class);

  private final ControllerContext _context;

  /**
   * Instantiate from a populated ControllerContext
   * @param context instantiated context
   */
  public ControllerContextHolder(ControllerContext context) {
    super(context.getId().toString());
    _context = context;
    StringSerializer serializer = instantiateSerializerFromContext(_context);
    if (_context != null && serializer != null) {
      _record.setSimpleField(Fields.SERIALIZER_CLASS.toString(), _context.getSerializerClass()
          .getName());
      _record.setSimpleField(Fields.CONTEXT_CLASS.toString(), _context.getClass().getName());
      _record.setSimpleField(Fields.CONTEXT.toString(), serializer.serialize(_context));
    }
  }

  /**
   * Instantiate from a record
   * @param record populated ZNRecord
   */
  public ControllerContextHolder(ZNRecord record) {
    super(record);
    StringSerializer serializer =
        instantiateSerializerFromName(_record.getSimpleField(Fields.SERIALIZER_CLASS.toString()));
    _context =
        loadContext(serializer, _record.getSimpleField(Fields.CONTEXT_CLASS.toString()),
            _record.getSimpleField(Fields.CONTEXT.toString()));
  }

  /**
   * Get a ControllerContext
   * @return instantiated {@link ControllerContext}
   */
  public ControllerContext getContext() {
    return _context;
  }

  /**
   * Get a ControllerContext as a specific subtyple
   * @return instantiated typed {@link ControllerContext}, or null
   */
  public <T extends ControllerContext> T getContext(Class<T> contextClass) {
    if (_context != null) {
      try {
        return contextClass.cast(_context);
      } catch (ClassCastException e) {
        LOG.warn(contextClass + " is incompatible with context class: " + _context.getClass());
      }
    }
    return null;
  }

  /**
   * Instantiate a StringSerializer that can serialize the context
   * @param context instantiated ControllerContext
   * @return StringSerializer object, or null if it could not be instantiated
   */
  private StringSerializer instantiateSerializerFromContext(ControllerContext context) {
    if (context == null) {
      return null;
    }
    try {
      return context.getSerializerClass().newInstance();
    } catch (InstantiationException e) {
      LOG.error("Serializer could not be instatiated", e);
    } catch (IllegalAccessException e) {
      LOG.error("Serializer default constructor not accessible", e);
    }
    return null;
  }

  /**
   * Instantiate a StringSerializer from its class name
   * @param serializerClassName name of a StringSerializer implementation class
   * @return instantiated StringSerializer, or null if it could not be instantiated
   */
  private StringSerializer instantiateSerializerFromName(String serializerClassName) {
    if (serializerClassName != null) {
      try {
        return (StringSerializer) HelixUtil.loadClass(getClass(), serializerClassName)
            .newInstance();
      } catch (InstantiationException e) {
        LOG.error("Error getting the serializer", e);
      } catch (IllegalAccessException e) {
        LOG.error("Error getting the serializer", e);
      } catch (ClassNotFoundException e) {
        LOG.error("Error getting the serializer", e);
      }
    }
    return null;
  }

  /**
   * Deserialize a context
   * @param serializer StringSerializer that can deserialize the context
   * @param className the name of the context class
   * @param contextData the raw context data as a String
   * @return ControllerContext, or null if there was a conversion issue
   */
  private ControllerContext loadContext(StringSerializer serializer, String className,
      String contextData) {
    if (serializer != null && className != null && contextData != null) {
      try {
        Class<? extends ControllerContext> contextClass =
            HelixUtil.loadClass(getClass(), className).asSubclass(ControllerContext.class);
        return serializer.deserialize(contextClass, contextData);
      } catch (ClassNotFoundException e) {
        LOG.error("Could not convert the persisted data into a " + className, e);
      } catch (ClassCastException e) {
        LOG.error("Could not convert the persisted data into a " + className, e);
      }
    }
    return null;
  }
}
