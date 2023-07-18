package org.apache.helix.metaclient.recipes.leaderelection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import java.io.ByteArrayInputStream;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LeaderInfoSerializer extends ZNRecordSerializer {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderInfoSerializer.class);

  @Override
  public Object deserialize(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      // reading a parent/null node
      return null;
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    mapper.enable(MapperFeature.AUTO_DETECT_FIELDS);
    mapper.enable(MapperFeature.AUTO_DETECT_SETTERS);
    mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    try {
      //decompress the data if its already compressed
      if (GZipCompressionUtil.isCompressed(bytes)) {
        byte[] uncompressedBytes = GZipCompressionUtil.uncompress(bais);
        bais = new ByteArrayInputStream(uncompressedBytes);
      }

      return mapper.readValue(bais, LeaderInfo.class);
    } catch (Exception e) {
      LOG.error("Exception during deserialization of bytes: {}", new String(bytes), e);
      return null;
    }
  }
}
