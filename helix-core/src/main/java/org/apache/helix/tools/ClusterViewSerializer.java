/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.tools;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.helix.ClusterView;
import org.apache.helix.manager.file.StaticFileHelixManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;


public class ClusterViewSerializer
{
  private static Logger logger = Logger.getLogger(ClusterViewSerializer.class);

  public static void serialize(ClusterView view, File file)
  {
    ObjectMapper mapper = new ObjectMapper();

    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_GETTERS, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    // serializationConfig.set(SerializationConfig.Feature.WRITE_NULL_PROPERTIES, true);

    try
    {
      mapper.writeValue(file, view);
    } 
    catch (Exception e)
    {
      logger.error("Error during serialization of data:" + view, e);
    }
  }
  
  public static byte[] serialize(ClusterView view)
  {
    ObjectMapper mapper = new ObjectMapper();

    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_GETTERS, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    // serializationConfig.set(SerializationConfig.Feature.WRITE_NULL_PROPERTIES, true);

    StringWriter sw = new StringWriter();

    try
    {
      mapper.writeValue(sw, view);
      return sw.toString().getBytes();
    } 
    catch (Exception e)
    {
      logger.error("Error during serialization of data:" + view, e);
    }

    return new byte[0];
  }

  public static ClusterView deserialize(File file)
  {
    if (!file.exists())
    {
      logger.error(String.format("Static config file \"%s\" doesn't exist", file.getAbsolutePath()));
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
   
    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    // deserializationConfig.set(DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    try
    {
      ClusterView view = mapper.readValue(file, ClusterView.class);
      return view;
    } 
    catch (Exception e)
    {
      logger.error("Error during deserialization of file:" + file.getAbsolutePath(), e);
    }

    return null;
  }

  
  public static ClusterView deserialize(byte[] bytes)
  {
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    // deserializationConfig.set(DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    try
    {
      ClusterView view = mapper.readValue(bais, ClusterView.class);
      return view;
    } 
    catch (Exception e)
    {
      logger.error("Error during deserialization of bytes:" + new String(bytes), e);
    }

    return null;
  }

  public static void main(String[] args) throws JsonGenerationException,
      JsonMappingException, IOException
  {
    // temporary test only
    // create fake db names and nodes
    List<StaticFileHelixManager.DBParam> dbParams = new ArrayList<StaticFileHelixManager.DBParam>();
    // dbParams.add(new FileBasedClusterManager.DBParam("BizFollow", 1));
    dbParams.add(new StaticFileHelixManager.DBParam("BizProfile_qatest218a", 128));
    // dbParams.add(new FileBasedClusterManager.DBParam("EspressoDB", 10));
    // dbParams.add(new FileBasedClusterManager.DBParam("MailboxDB", 128));
    // dbParams.add(new FileBasedClusterManager.DBParam("MyDB", 8));
    // dbParams.add(new FileBasedClusterManager.DBParam("schemata", 1));
    String[] nodesInfo = { "localhost:8900", "localhost:8901",
                           "localhost:8902", "localhost:8903",
                           "localhost:8904" };
    // String[] nodesInfo = { "esv4-app75.stg.linkedin.com:12918" };
    int replica = 0;

    ClusterView view = StaticFileHelixManager.generateStaticConfigClusterView(nodesInfo, dbParams, replica);
    String file = "/tmp/cluster-view-bizprofile.json";
    // ClusterViewSerializer serializer = new ClusterViewSerializer(file);

    byte[] bytes = ClusterViewSerializer.serialize(view);
    // logger.info("serialized bytes=" );
    // logger.info(new String(bytes));
    System.out.println("serialized bytes=");
    System.out.println(new String(bytes));

    ClusterView restoredView = ClusterViewSerializer.deserialize(bytes);
    // logger.info(restoredView);

    bytes = ClusterViewSerializer.serialize(restoredView);
    // logger.info(new String(bytes));
    System.out.println("restored cluster view=");
    System.out.println(new String(bytes));

  }
}
