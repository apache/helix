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
<#if (resourceStateTable?size == 0)>
    <div class="uk-alert uk-alert-warning">
        <p>
            No partitions of ${activeResource} are assigned!
        </p>
    </div>
<#else>
    <div class="filter-area">
        <p>
            <i>Filter</i> <button id="filter-add" class="uk-button uk-button-mini">+</button>
        </p>
        <form id="filter-form" class="uk-form"></form>
    </div>

    <table class="uk-table" id="resource-state-table">
        <thead>
            <tr>
                <th>Partition</th>
                <th>Instance</th>
                <th>Ideal</th>
                <th>External</th>
            </tr>
        </thead>
        <tbody>
            <#list resourceStateTable as row>
                <tr class="${(row.ideal == row.external)?string("state-ok", "state-warn")} ${(row.external == "ERROR")?string("state-error", "")}">
                    <td>${row.partitionName}</td>
                    <td>${row.instanceName}</td>
                    <td>${row.ideal}</td>
                    <td>${row.external}</td>
                </tr>
            </#list>
        </tbody>
    </table>
</#if>
