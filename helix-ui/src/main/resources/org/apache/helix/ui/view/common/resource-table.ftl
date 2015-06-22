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
<#if (activeClusterResources?size == 0)>
    <div class="uk-alert">
        <p>
            There are no resources in ${activeCluster}
        </p>
    </div>
<#else>
    <div class="filter-area">
        <p>
            <i>Filter</i> <button id="resource-filter-add" class="uk-button uk-button-mini">+</button>
        </p>
        <form id="resource-filter-form" class="uk-form"></form>
    </div>

    <table class="uk-table" id="resource-table">
        <thead>
            <th>Name</th>
            <#if (adminMode)>
                <th></th>
            </#if>
        </thead>
        <tbody>
            <#list activeClusterResources as resource>
                <tr>
                    <td><a href="/dashboard/${zkAddress}/${activeCluster}/${resource}">${resource}</a></td>
                    <#if (adminMode)>
                        <td class="table-button">
                            <button class="uk-button uk-button-danger drop-resource-button" cluster="${activeCluster}" resource="${resource}">Drop</button>
                        </td>
                    </#if>
                </tr>
            </#list>
        </tbody>
    </table>
</#if>
