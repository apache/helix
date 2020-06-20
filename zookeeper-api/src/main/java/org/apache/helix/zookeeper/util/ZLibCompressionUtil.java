package org.apache.helix.zookeeper.util;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides support for general purpose compression using the popular ZLIB compression.
 * It uses {@link Deflater} to compress.
 */
public class ZLibCompressionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ZLibCompressionUtil.class);

  /**
   * Compresses input byte array using {@link Deflater} compression.
   *
   * @param bytes byte array to be compressed
   * @return compressed byte array
   * @throws IOException if an I/O error occurs
   */
  public static byte[] compress(byte[] bytes) throws IOException {
    Deflater df = new Deflater();
    df.setInput(bytes);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);
    df.finish();

    byte[] buff = new byte[1024];
    while (!df.finished())
    {
      int count = df.deflate(buff);
      baos.write(buff, 0, count);
    }
    baos.close();
    byte[] compressedBytes = baos.toByteArray();

    LOG.debug("Compress: decompressed size: {}, compressed size: {}", bytes.length,
        compressedBytes.length);

    return compressedBytes;
  }

  /**
   * Decompresses byte array using {@link Inflater} decompression.
   *
   * @param bytes compressed byte array to be decompressed
   * @return decompressed byte array
   * @throws IOException if an I/O error occurs
   * @throws DataFormatException if the compressed data format is invalid
   */
  public static byte[] decompress(byte[] bytes) throws IOException, DataFormatException {
    Inflater decompressor = new Inflater();
    decompressor.setInput(bytes);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);

    byte[] buff = new byte[1024];
    while (!decompressor.finished()) {
      int count = decompressor.inflate(buff);
      baos.write(buff, 0, count);
    }
    baos.close();

    byte[] decompressedBytes = baos.toByteArray();

    LOG.debug("Decompress: decompressed size: {}, compressed size: {}", decompressedBytes.length,
        bytes.length);

    return decompressedBytes;
  }
}
