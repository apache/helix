package org.apache.helix.zookeeper.introspect;

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties.Value;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Typing;
import com.fasterxml.jackson.databind.annotation.NoClass;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.AnnotatedParameter;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;

/**
 * This introspector works with jackson 1 annotations used in
 * {@link org.apache.helix.zookeeper.datamodel.ZNRecord}.
 * This is supposed used ONLY in Helix as it could be deprecated in the next major release.
 */
// TODO: remove this class once upgrading to Jackson 2.x in ZNRecord
public class CodehausJacksonIntrospector extends NopAnnotationIntrospector {
  private static final long serialVersionUID = 1L;

  public Value findPropertyIgnorals(Annotated a) {
    JsonIgnoreProperties ignoreAnnotation = a.getAnnotation(JsonIgnoreProperties.class);
    if (ignoreAnnotation != null) {
      return Value.forIgnoreUnknown(ignoreAnnotation.ignoreUnknown());
    }
    return super.findPropertyIgnorals(a);
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findDeserializationContentType(Annotated a, JavaType baseContentType) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      Class<?> serClass = serializeAnnotation.contentAs();
      if (serClass != NoClass.class) {
        return serClass;
      }
    }
    return null;
  }

  @Override
  public PropertyName findNameForDeserialization(Annotated a) {
    String name = null;
    if (a instanceof AnnotatedField) {
      name = findDeserializationName((AnnotatedField) a);
    } else if (a instanceof AnnotatedMethod) {
      name = findDeserializationName((AnnotatedMethod) a);
    } else if (a instanceof AnnotatedParameter) {
      name = findDeserializationName((AnnotatedParameter) a);
    }
    if (name == null) {
      return null;
    }
    return name.isEmpty() ? PropertyName.USE_DEFAULT : new PropertyName(name);
  }

  public String findDeserializationName(AnnotatedField af) {
    JsonProperty propertyAnnotation = af.getAnnotation(JsonProperty.class);
    return propertyAnnotation == null ? null : propertyAnnotation.value();
  }

  public String findDeserializationName(AnnotatedMethod am) {
    JsonProperty propertyAnnotation = am.getAnnotation(JsonProperty.class);
    return propertyAnnotation == null ? null : propertyAnnotation.value();
  }

  public String findDeserializationName(AnnotatedParameter param) {
    if (param != null) {
      JsonProperty propertyAnnotation = param.getAnnotation(JsonProperty.class);
      if (propertyAnnotation != null) {
        return propertyAnnotation.value();
      }
    }
    return null;
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findSerializationContentType(Annotated a, JavaType baseType) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      Class<?> serClass = serializeAnnotation.contentAs();
      if (serClass != NoClass.class) {
        return serClass;
      }
    }
    return null;
  }

  /*
   * Handles JsonSerialize.Inclusion
   */
  @Override
  public JsonInclude.Value findPropertyInclusion(Annotated a) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      for (Include include : Include.values()) {
        if (include.name().equals(serializeAnnotation.include().name())) {
          return JsonInclude.Value.construct(include, include);
        }
      }
    }
    return JsonInclude.Value.empty();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Include findSerializationInclusion(Annotated a, Include defValue) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    return serializeAnnotation == null ? defValue
        : Include.valueOf(serializeAnnotation.include().name());
  }

  @SuppressWarnings("deprecation")
  @Override
  public Include findSerializationInclusionForContent(Annotated a, JsonInclude.Include defValue) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    return serializeAnnotation == null ? defValue
        : Include.valueOf(serializeAnnotation.include().name());
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findSerializationKeyType(Annotated a, JavaType baseType) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      Class<?> serClass = serializeAnnotation.keyAs();
      if (serClass != NoClass.class) {
        return serClass;
      }
    }
    return null;
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findSerializationType(Annotated a) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      Class<?> serClass = serializeAnnotation.as();
      if (serClass != NoClass.class) {
        return serClass;
      }
    }
    return null;
  }

  @Override
  public Typing findSerializationTyping(Annotated a) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    return serializeAnnotation == null ? null : Typing.valueOf(serializeAnnotation.typing().name());
  }

  @Override
  public PropertyName findNameForSerialization(Annotated a) {
    String name;
    if (a instanceof AnnotatedField) {
      name = findSerializationName((AnnotatedField) a);
    } else if (a instanceof AnnotatedMethod) {
      name = findSerializationName((AnnotatedMethod) a);
    } else {
      name = null;
    }
    if (name != null) {
      if (name.length() == 0) { // empty String means 'default'
        return PropertyName.USE_DEFAULT;
      }
      return new PropertyName(name);
    }
    return null;
  }

  public String findSerializationName(AnnotatedField af) {
    JsonProperty propertyAnnotation = af.getAnnotation(JsonProperty.class);
    if (propertyAnnotation != null) {
      return propertyAnnotation.value();
    }

    if (af.hasAnnotation(JsonSerialize.class)) {
      return "";
    }
    return null;
  }

  public String findSerializationName(AnnotatedMethod am) {
    JsonProperty propertyAnnotation = am.getAnnotation(JsonProperty.class);
    if (propertyAnnotation != null) {
      return propertyAnnotation.value();
    }

    if (am.hasAnnotation(JsonSerialize.class)) {
      return "";
    }
    return null;
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean hasCreatorAnnotation(Annotated a) {
    return a.hasAnnotation(JsonCreator.class);
  }

  @Override
  public boolean hasIgnoreMarker(AnnotatedMember am) {
    JsonIgnore ignoreAnnotation = am.getAnnotation(JsonIgnore.class);
    return (ignoreAnnotation != null && ignoreAnnotation.value());
  }

  /*
   * handling of serializers
   */
  @Override
  public Object findSerializer(Annotated a) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      Class<? extends JsonSerializer> serClass = serializeAnnotation.using();
      if (serClass != JsonSerializer.None.class) {
        return serClass;
      }
    }
    return null;
  }

  @Override
  public Object findKeySerializer(Annotated a) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      Class<? extends JsonSerializer> serClass = serializeAnnotation.keyUsing();
      if (serClass != JsonSerializer.None.class) {
        return serClass;
      }
    }
    return null;
  }

  @Override
  public Object findContentSerializer(Annotated a) {
    JsonSerialize serializeAnnotation = a.getAnnotation(JsonSerialize.class);
    if (serializeAnnotation != null) {
      Class<? extends JsonSerializer> serClass = serializeAnnotation.contentUsing();
      if (serClass != JsonSerializer.None.class) {
        return serClass;
      }
    }
    return null;
  }
}
