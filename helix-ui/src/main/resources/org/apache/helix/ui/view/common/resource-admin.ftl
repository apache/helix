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
