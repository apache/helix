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
