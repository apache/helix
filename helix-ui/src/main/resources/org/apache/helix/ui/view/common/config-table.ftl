<#if (configTable?size == 0)>
    <div class="uk-alert">
        <p>
            No Helix configuration available for ${activeResource!activeCluster}
        </p>
    </div>
<#else>
    <table class="uk-table">
        <thead>
            <tr>
                <th>Name</th>
                <th>Value</th>
            </tr>
        </thead>
        <tbody>
            <#list configTable as row>
                <tr>
                    <td>${row.name}</td>
                    <td>${row.value}</td>
                </tr>
            </#list>
        </tbody>
    </table>
</#if>
