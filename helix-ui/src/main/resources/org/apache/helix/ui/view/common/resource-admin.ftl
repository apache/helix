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
<form id="resource-add-form" class="uk-form uk-form-horizontal">
    <div class="uk-form-row">
        <span class="uk-form-label">Name</span>
        <input type="text" placeholder="MyResource" id="resource-name" />
    </div>

    <div class="uk-form-row">
        <span class="uk-form-label">Partitions</span>
        <input id="resource-partitions" type="number"/>
    </div>

    <div class="uk-form-row">
        <span class="uk-form-label">Replicas</span>
        <input id="resource-replicas" type="number"/>
    </div>

    <div class="uk-form-row">
        <span class="uk-form-label">State Model</span>
        <select id="resource-state-model">
            <#list stateModels as stateModel>
                <option>${stateModel}</option>
            </#list>
        </select>
    </div>

    <div class="uk-form-row">
        <span class="uk-form-label">Rebalance Mode</span>
        <select id="resource-rebalance-mode">
            <#list rebalanceModes as rebalanceMode>
                <option>${rebalanceMode}</option>
            </#list>
        </select>
    </div>

    <div class="uk-form-row">
        <span class="uk-form-label"></span>
        <button id="add-resource-button" class="uk-button uk-button-primary" cluster="${activeCluster}">Add</button>
    </div>
</form>
