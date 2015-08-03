package org.apache.helix.ipc.netty;

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

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class NettyHelixIPCUtils {
  /** Writes [s.length(), s] to buf, or [0] if s is null */
  public static void writeStringWithLength(ByteBuf buf, String s) {
    if (s == null) {
      buf.writeInt(0);
      return;
    }

    buf.writeInt(s.length());
    for (int i = 0; i < s.length(); i++) {
      buf.writeByte(s.charAt(i));
    }
  }

  /** Returns the length of a string, or 0 if s is null */
  public static int getLength(String s) {
    return s == null ? 0 : s.length();
  }

  /** Given a byte buf w/ a certain reader index, encodes the next length bytes as a String */
  public static String toNonEmptyString(int length, ByteBuf byteBuf) {
    if (byteBuf.readableBytes() >= length) {
      String string =  byteBuf.toString(byteBuf.readerIndex(), length, Charset.defaultCharset());
      byteBuf.readerIndex(byteBuf.readerIndex() + length);
      return string;
    }
    return null;
  }

  /**
   * @throws java.lang.IllegalArgumentException if length > messageLength (attempt to prevent OOM
   *           exceptions)
   */
  public static void checkLength(String fieldName, int length, int messageLength)
      throws IllegalArgumentException {
    if (length > messageLength) {
      throw new IllegalArgumentException(fieldName + "=" + length
          + " is greater than messageLength=" + messageLength);
    }
  }
}
