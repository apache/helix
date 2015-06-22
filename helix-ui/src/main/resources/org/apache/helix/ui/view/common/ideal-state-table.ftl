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
<table class="uk-table">
    <tr>
        <td class="ideal-state-prop">Partitions</td>
        <td>${idealStateSpec.numPartitions}</td>
    </tr>
    <tr>
        <td class="ideal-state-prop">Replicas</td>
        <td>${idealStateSpec.replicas}</td>
    </tr>
    <#if (idealStateSpec.instanceGroupTag??)>
        <tr>
            <td class="ideal-state-prop">Tag</td>
            <td>${idealStateSpec.instanceGroupTag}</td>
        </tr>
    </#if>
    <tr>
        <td class="ideal-state-prop">Rebalance Mode</td>
        <td>${idealStateSpec.rebalanceMode}</td>
    </tr>
    <#if (idealStateSpec.rebalancerClassName??)>
        <tr>
            <td class="ideal-state-prop">Rebalancer Class</td>
            <td>${idealStateSpec.rebalancerClassName}</td>
        </tr>
    </#if>
    <tr>
        <td class="ideal-state-prop">State Model</td>
        <td>${idealStateSpec.stateModel}</td>
    </tr>
    <tr>
        <td class="ideal-state-prop">Batch Messaging</td>
        <td>${idealStateSpec.batchMessageMode?string("true", "false")}</td>
    </tr>
</table>
