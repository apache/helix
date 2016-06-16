/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.helix.controller.rebalancer.strategy.crushMapping;

public class JenkinsHash {
  // max value to limit it to 4 bytes
  private static final long MAX_VALUE = 0xFFFFFFFFL;
  private static final long CRUSH_HASH_SEED = 1315423911L;

  /**
   * Convert a byte into a long value without making it negative.
   */
  private static long byteToLong(byte b) {
    long val = b & 0x7F;
    if ((b & 0x80) != 0) {
      val += 128;
    }
    return val;
  }

  /**
   * Do addition and turn into 4 bytes.
   */
  private static long add(long val, long add) {
    return (val + add) & MAX_VALUE;
  }

  /**
   * Do subtraction and turn into 4 bytes.
   */
  private static long subtract(long val, long subtract) {
    return (val - subtract) & MAX_VALUE;
  }

  /**
   * Left shift val by shift bits and turn in 4 bytes.
   */
  private static long xor(long val, long xor) {
    return (val ^ xor) & MAX_VALUE;
  }

  /**
   * Left shift val by shift bits.  Cut down to 4 bytes.
   */
  private static long leftShift(long val, int shift) {
    return (val << shift) & MAX_VALUE;
  }

  /**
   * Convert 4 bytes from the buffer at offset into a long value.
   */
  private static long fourByteToLong(byte[] bytes, int offset) {
    return (byteToLong(bytes[offset + 0])
        + (byteToLong(bytes[offset + 1]) << 8)
        + (byteToLong(bytes[offset + 2]) << 16)
        + (byteToLong(bytes[offset + 3]) << 24));
  }

  /**
   * Mix up the values in the hash function.
   */
  private static Triple hashMix(Triple t) {
    long a = t.a; long b = t.b; long c = t.c;
    a = subtract(a, b); a = subtract(a, c); a = xor(a, c >> 13);
    b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 8));
    c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 13));
    a = subtract(a, b); a = subtract(a, c); a = xor(a, (c >> 12));
    b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 16));
    c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 5));
    a = subtract(a, b); a = subtract(a, c); a = xor(a, (c >> 3));
    b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 10));
    c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 15));
    return new Triple(a, b, c);
  }

  private static class Triple {
    long a;
    long b;
    long c;

    public Triple(long a, long b, long c) {
      this.a = a; this.b = b; this.c = c;
    }
  }

  public long hash(long a) {
    long hash = xor(CRUSH_HASH_SEED, a);
    long b = a;
    long x = 231232L;
    long y = 1232L;
    Triple val = hashMix(new Triple(b, x, hash));
    b = val.a; x = val.b; hash = val.c;
    val = hashMix(new Triple(y, a, hash));
    hash = val.c;
    return hash;
  }

  public long hash(long a, long b) {
    long hash = xor(xor(CRUSH_HASH_SEED, a), b);
    long x = 231232L;
    long y = 1232L;
    Triple val = hashMix(new Triple(a, b, hash));
    a = val.a; b = val.b; hash = val.c;
    val = hashMix(new Triple(x, a, hash));
    x = val.a; a = val.b; hash = val.c;
    val = hashMix(new Triple(b, y, hash));
    hash = val.c;
    return hash;
  }

  public long hash(long a, long b, long c) {
    long hash = xor(xor(xor(CRUSH_HASH_SEED, a), b), c);
    long x = 231232L;
    long y = 1232L;
    Triple val = hashMix(new Triple(a, b, hash));
    a = val.a; b = val.b; hash = val.c;
    val = hashMix(new Triple(c, x, hash));
    c = val.a; x = val.b; hash = val.c;
    val = hashMix(new Triple(y, a, hash));
    y = val.a; a = val.b; hash = val.c;
    val = hashMix(new Triple(b, x, hash));
    b = val.a; x = val.b; hash = val.c;
    val = hashMix(new Triple(y, c, hash));
    hash = val.c;
    return hash;
  }
}
