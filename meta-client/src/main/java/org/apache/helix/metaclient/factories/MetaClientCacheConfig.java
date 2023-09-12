package org.apache.helix.metaclient.factories;

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



public class MetaClientCacheConfig {
    private final String _rootEntry;
    private boolean _cacheData = false;
    private boolean _cacheChildren = false;
    private boolean _lazyCaching = true;

    public MetaClientCacheConfig(String rootEntry, boolean cacheData, boolean cacheChildren, boolean lazyCaching) {
        _rootEntry = rootEntry;
        _cacheData = cacheData;
        _cacheChildren = cacheChildren;
        _lazyCaching = lazyCaching;
    }

    public String getRootEntry() {
        return _rootEntry;
    }

    public boolean getCacheData() {
        return _cacheData;
    }

    public boolean getCacheChildren() {
        return _cacheChildren;
    }

    public boolean getLazyCaching() {
        return _lazyCaching;
    }
}
