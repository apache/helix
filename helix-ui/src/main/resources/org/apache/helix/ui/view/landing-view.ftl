<!DOCTYPE html>
<html>
    <head>
        <title>Helix</title>
        <meta charset="UTF-8">
        <#include "common/css.ftl">
    </head>

    <body>
        <div id="landing-area">
            <img src="/assets/img/helix-logo.png">
            <form id="landing-form" class="uk-form">
                <input id="zk-address" type="text" placeholder="ZooKeeper Address (e.g. localhost:2181)"/>
                <button id="landing-form-button" class="uk-button">Go</button>
            </form>
        </div>

        <#include "common/js.ftl">
        <script src="/assets/js/landing-view.js"></script>
    </body>
</html>
