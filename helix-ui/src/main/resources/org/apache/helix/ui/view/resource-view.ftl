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
        <title>${zkAddress}</title>
        <meta charset="UTF-8">
        <#include "common/css.ftl">
    </head>

    <body>

        <div class="uk-grid">
            <div class="uk-width-medium-2-10">
                <#include "common/side-nav.ftl">
            </div>

            <div class="uk-width-medium-8-10">
                <div id="cluster-views-area">
                    <#if (activeValid)>
                        <h1><a href="/dashboard/${zkAddress}/${activeCluster}">${activeCluster}</a> / ${activeResource}</h1>

                        <ul id="switcher-tabs" class="uk-subnav uk-subnav-pill" data-uk-switcher="{connect: '#switcher'}">
                            <li><a href="">Partitions</a></li>
                            <li><a href="">Visualizer</a></li>
                            <li><a href="">Config</a></li>
                        </ul>

                        <ul id="switcher" class="uk-switcher">
                            <li><#include "common/resource-state-table.ftl"></li>
                            <li><#include "common/resource-visualizer.ftl"></li>
                            <li>
                                <#include "common/ideal-state-table.ftl">
                                <#include "common/config-table.ftl">
                            </li>
                        </ul>
                    <#else>
                        <div class="uk-alert uk-alert-danger">
                            <p>
                                ${activeCluster} is an invalid Helix cluster
                            </p>
                        </div>
                    </#if>
                </div>
            </div>
        </div>

        <#include "common/js.ftl">
        <script src="/assets/js/resource-state-table.js"></script>
    </body>
</html>
