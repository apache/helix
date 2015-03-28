<!DOCTYPE html>
<html>
    <head>
        <title>${zkAddress}</title>
        <meta charset="UTF-8">
        <#include "common/css.ftl">
    </head>

    <body>

        <div class="uk-grid">
            <div class="uk-width-medium-2-10">
                <#include "common/side-nav.ftl">
            </div>

            <div class="uk-width-medium-8-10">
                <div id="cluster-views-area">
                    <#if (activeValid)>
                        <h1>${activeCluster}</h1>

                        <ul id="switcher-tabs" class="uk-subnav uk-subnav-pill" data-uk-switcher="{connect: '#switcher'}">
                            <li><a href="">Resources</a></li>
                            <li><a href="">Instances</a></li>
                            <li><a href="">Config</a></li>
                        </ul>

                        <ul id="switcher" class="uk-switcher">
                            <li>
                                <#include "common/resource-table.ftl">
                                <#if (adminMode)>
                                    <#include "common/resource-admin.ftl">
                                </#if>
                            </li>
                            <li>
                                <#include "common/instance-table.ftl">
                                <#if (adminMode)>
                                    <#include "common/instance-admin.ftl">
                                </#if>
                             </li>
                            <li>
                                <#include "common/config-table.ftl">
                                <#if (adminMode)>
                                    <#include "common/cluster-admin.ftl">
                                </#if>
                            </li>
                        </ul>
                    <#else>
                        <div class="uk-alert uk-alert-danger">
                            <p>
                                ${activeCluster} is an invalid Helix cluster
                            </p>
                        </div>
                    </#if>
                </div>
            </div>
        </div>

        <#include "common/js.ftl">
        <script src="/assets/js/resource-table.js"></script>
    </body>
</html>
