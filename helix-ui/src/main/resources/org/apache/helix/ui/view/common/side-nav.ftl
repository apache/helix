<!-- Side nav (clusters) -->
<div class="uk-panel uk-panel-box">
    <h3 id="panel-helix-logo" class="uk-panel-title">
        <a href="/dashboard">
            <img src="/assets/img/helix-logo.png" alt="Helix"/>
        </a>
    </h3>

    <ul class="uk-nav uk-nav-side">
        <#list clusters as cluster>
            <li class="${(cluster == activeCluster)?string("uk-active", "")}"><a href="/dashboard/${zkAddress}/${cluster}">${cluster}</a></li>
        </#list>
    </ul>

    <#if (adminMode)>
        <form id="add-cluster-form" class="uk-form">
            <input type="text" placeholder="CLUSTER_NAME"/>
            <button class="uk-button">Add</button>
        </form>
    </#if>
</div>
