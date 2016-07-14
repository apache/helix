<#--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<!DOCTYPE html>
<html>
    <head>
        <title>Helix</title>
        <meta charset="UTF-8">
        <#include "common/css.ftl">
    </head>

    <body>
        <div id="landing-area">
            <img src="/assets/img/helix-logo.png">
            <form id="landing-form" class="uk-form">
                <div class="uk-form-row">
                    <input id="zk-address"
                           type="text"
                           placeholder="ZooKeeper Address (e.g. localhost:2181)"
                           class="uk-form-width-large"/>
                </div>
                <#if zkAliases?has_content>
                    <div class="uk-form-row">
                        OR
                    </div>
                    <div class="uk-form-row">
                        <select id="zk-alias"
                                class="uk-form-width-large">
                            <#list zkAliases as alias>
                                <option value="${alias}">${alias}</option>
                            </#list>
                        </select>
                    </div>
                </#if>
                <div class="uk-form-row">
                    <button id="landing-form-button"
                            class="uk-button uk-form-width-large">
                        Go
                    </button>
                </div>
            </form>
        </div>

        <#include "common/js.ftl">
        <script src="/assets/js/landing-view.js"></script>
    </body>
</html>
