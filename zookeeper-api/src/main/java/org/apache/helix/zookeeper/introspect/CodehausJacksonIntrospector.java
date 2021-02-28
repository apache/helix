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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Typing;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.AnnotatedParameter;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.annotate.NoClass;

/**
 * This introspector works with jackson 1 annotations used in
 * {@link org.apache.helix.zookeeper.datamodel.ZNRecord}.
 * This is supposed used ONLY in Helix as it could be deprecated in the next major release.
 */
// TODO: remove this class once upgrading to Jackson 2.x in ZNRecord
public class CodehausJacksonIntrospector extends NopAnnotationIntrospector {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("deprecation")
  @Override
  public String findEnumValue(Enum<?> value) {
    return value.name();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Boolean findIgnoreUnknownProperties(AnnotatedClass ac) {
    JsonIgnoreProperties ignore = ac.getAnnotation(JsonIgnoreProperties.class);
    return (ignore == null) ? null : ignore.ignoreUnknown();
  }

  @SuppressWarnings("deprecation")
  @Override
  public String[] findPropertiesToIgnore(Annotated ac) {
    JsonIgnoreProperties ignore = ac.getAnnotation(JsonIgnoreProperties.class);
    return (ignore == null) ? null : ignore.value();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findDeserializationContentType(Annotated am, JavaType baseContentType) {
    JsonSerialize ann = am.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      Class<?> cls = ann.contentAs();
      if (cls != NoClass.class) {
        return cls;
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
    JsonProperty pann = af.getAnnotation(JsonProperty.class);
    return pann == null ? null : pann.value();
  }

  public String findDeserializationName(AnnotatedMethod am) {
    JsonProperty pann = am.getAnnotation(JsonProperty.class);
    return pann == null ? null : pann.value();
  }

  public String findDeserializationName(AnnotatedParameter param) {
    if (param != null) {
      JsonProperty pann = param.getAnnotation(JsonProperty.class);
      if (pann != null) {
        return pann.value();
      }
    }
    return null;
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findSerializationContentType(Annotated am, JavaType baseType) {
    JsonSerialize ann = am.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      Class<?> cls = ann.contentAs();
      if (cls != NoClass.class) {
        return cls;
      }
    }
    return null;
  }

  /*
   * Handles JsonSerialize.Inclusion
   */
  @Override
  public JsonInclude.Value findPropertyInclusion(Annotated a) {
    JsonSerialize ann = a.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      for (Include include : Include.values()) {
        if (include.name().equals(ann.include().name())) {
          return JsonInclude.Value.construct(include, include);
        }
      }
    }
    return JsonInclude.Value.empty();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Include findSerializationInclusion(Annotated a, Include defValue) {
    JsonSerialize ann = a.getAnnotation(JsonSerialize.class);
    return ann == null ? defValue : Include.valueOf(ann.include().name());
  }

  @SuppressWarnings("deprecation")
  @Override
  public Include findSerializationInclusionForContent(Annotated a, JsonInclude.Include defValue) {
    JsonSerialize ann = a.getAnnotation(JsonSerialize.class);
    return ann == null ? defValue : Include.valueOf(ann.include().name());
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findSerializationKeyType(Annotated am, JavaType baseType) {
    JsonSerialize ann = am.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      Class<?> cls = ann.keyAs();
      if (cls != NoClass.class) {
        return cls;
      }
    }
    return null;
  }

  @SuppressWarnings("deprecation")
  @Override
  public Class<?> findSerializationType(Annotated a) {
    JsonSerialize ann = a.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      Class<?> cls = ann.as();
      if (cls != NoClass.class) {
        return cls;
      }
    }
    return null;
  }

  @Override
  public Typing findSerializationTyping(Annotated a) {
    JsonSerialize ann = a.getAnnotation(JsonSerialize.class);
    return ann == null ? null : Typing.valueOf(ann.typing().name());
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
    JsonProperty pann = af.getAnnotation(JsonProperty.class);
    if (pann != null) {
      return pann.value();
    }

    if (af.hasAnnotation(JsonSerialize.class)) {
      return "";
    }
    return null;
  }

  @SuppressWarnings("deprecation")
  public String findSerializationName(AnnotatedMethod am) {
    JsonProperty pann = am.getAnnotation(JsonProperty.class);
    if (pann != null) {
      return pann.value();
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
  public boolean hasIgnoreMarker(AnnotatedMember m) {
    JsonIgnore ann = m.getAnnotation(JsonIgnore.class);
    return (ann != null && ann.value());
  }

  /*
   * handling of serializers
   */
  @Override
  public Object findSerializer(Annotated am) {
    JsonSerialize ann = am.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      Class<? extends JsonSerializer<?>> serClass = ann.using();
      if (serClass != JsonSerializer.None.class) {
        return serClass;
      }
    }
    return null;
  }

  @Override
  public Object findKeySerializer(Annotated am) {
    JsonSerialize ann = am.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      Class<? extends JsonSerializer<?>> serClass = ann.keyUsing();
      if (serClass != JsonSerializer.None.class) {
        return serClass;
      }
    }
    return null;
  }

  @Override
  public Object findContentSerializer(Annotated am) {
    JsonSerialize ann = am.getAnnotation(JsonSerialize.class);
    if (ann != null) {
      Class<? extends JsonSerializer<?>> serClass = ann.contentUsing();
      if (serClass != JsonSerializer.None.class) {
        return serClass;
      }
    }
    return null;
  }
}
