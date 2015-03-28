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
