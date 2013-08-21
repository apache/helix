package org.apache.helix.taskexecution;

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

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTaskResultStore implements TaskResultStore {
  private JedisPool _jedisPool;

  public RedisTaskResultStore(String redisServer, int redisPort, int timeout) {
    _jedisPool = new JedisPool(new JedisPoolConfig(), redisServer, redisPort, timeout);
  }

  @Override
  public boolean exists(String key) throws Exception {
    Jedis jedis = _jedisPool.getResource();
    try {
      return jedis.exists(key);
    } finally {
      _jedisPool.returnResource(jedis);
    }
  }

  @Override
  public void rpush(String key, String value) throws Exception {
    Jedis jedis = _jedisPool.getResource();
    try {
      jedis.rpush(key, value);
    } finally {
      _jedisPool.returnResource(jedis);
    }
  }

  @Override
  public List<String> lrange(String key, long start, long end) throws Exception {
    Jedis jedis = _jedisPool.getResource();
    try {
      return jedis.lrange(key, start, end);
    } finally {
      _jedisPool.returnResource(jedis);
    }
  }

  @Override
  public void ltrim(String key, long start, long end) throws Exception {
    Jedis jedis = _jedisPool.getResource();
    try {
      jedis.ltrim(key, start, end);
    } finally {
      _jedisPool.returnResource(jedis);
    }
  }

  @Override
  public long llen(String key) throws Exception {
    Jedis jedis = _jedisPool.getResource();
    try {
      return jedis.llen(key);
    } finally {
      _jedisPool.returnResource(jedis);
    }
  }

  @Override
  public Long hincrBy(String key, String field, long value) throws Exception {
    Jedis jedis = _jedisPool.getResource();
    try {
      return jedis.hincrBy(key, field, value);
    } finally {
      _jedisPool.returnResource(jedis);
    }
  }

  @Override
  public Map<String, String> hgetAll(String key) throws Exception {
    Jedis jedis = _jedisPool.getResource();
    try {
      return jedis.hgetAll(key);
    } finally {
      _jedisPool.returnResource(jedis);
    }
  }
}
