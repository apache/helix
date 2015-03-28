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
