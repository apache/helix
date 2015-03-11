package org.apache.helix.util;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZipCompressionUtil {
  /**
   * Compresses a byte array by applying GZIP compression
   * @param serializedBytes
   * @return
   * @throws IOException
   */
  public static byte[] compress(byte[] buffer) throws IOException {
    ByteArrayOutputStream gzipByteArrayOutputStream = new ByteArrayOutputStream();
    GZIPOutputStream gzipOutputStream = null;
    gzipOutputStream = new GZIPOutputStream(gzipByteArrayOutputStream);
    gzipOutputStream.write(buffer, 0, buffer.length);
    gzipOutputStream.close();
    byte[] compressedBytes = gzipByteArrayOutputStream.toByteArray();
    return compressedBytes;
  }

  public static byte[] uncompress(ByteArrayInputStream bais) throws IOException {
    GZIPInputStream gzipInputStream = new GZIPInputStream(bais);
    byte[] buffer = new byte[1024];
    int length;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    while ((length = gzipInputStream.read(buffer)) != -1) {
      baos.write(buffer, 0, length);
    }
    gzipInputStream.close();
    baos.close();
    byte[] uncompressedBytes = baos.toByteArray();
    return uncompressedBytes;
  }

  /*
   * Determines if a byte array is compressed. The java.util.zip GZip
   * implementaiton does not expose the GZip header so it is difficult to determine
   * if a string is compressed.
   * @param bytes an array of bytes
   * @return true if the array is compressed or false otherwise
   */
  public static boolean isCompressed(byte[] bytes) {
    if ((bytes == null) || (bytes.length < 2)) {
      return false;
    } else {
      return ((bytes[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8)));
    }
  }
}
