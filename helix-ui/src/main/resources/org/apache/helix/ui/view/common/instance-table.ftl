<#if (instanceSpecs?size == 0)>
    <div class="uk-alert">
        <p>
            There are no instances assigned to ${activeResource!activeCluster}
        </p>
    </div>
<#else>
    <table class="uk-table">
        <thead>
            <tr>
                <th>Instance</th>
                <th>Enabled</th>
                <th>Live</th>
                <#if (adminMode)>
                    <th></th>
                </#if>
            </tr>
        </thead>
        <tbody>
            <#list instanceSpecs as instanceSpec>
                <tr class="${instanceSpec.enabled?string("instance-enabled", "instance-disabled")} ${instanceSpec.live?string("", "instance-down")}">
                    <td>${instanceSpec.instanceName}</td>
                    <td>${instanceSpec.enabled?string("Yes", "No")}</td>
                    <td>${instanceSpec.live?string("Yes", "No")}</td>
                    <#if (adminMode)>
                        <td class="table-button">
                            <div class="uk-button-group">
                                <button class="uk-button enable-instance-button" cluster="${activeCluster}" instance="${instanceSpec.instanceName}">Enable</button>
                                <button class="uk-button disable-instance-button" cluster="${activeCluster}" instance="${instanceSpec.instanceName}">Disable</button>
                                <button class="uk-button uk-button-danger drop-instance-button" cluster="${activeCluster}" instance="${instanceSpec.instanceName}">Drop</button>
                            </div>
                        </td>
                    </#if>
                </tr>
            </#list>
        </tbody>
    </table>
</#if>
