/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

$(document).ready(function() {
    $("#add-cluster-form button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)
        var clusterName = $("#add-cluster-form input").val()

        if (confirm("Are you sure you want to add cluster " + clusterName + "?")) {
            var path = "/admin/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
            $("body").css("cursor", "progress")
            $.ajax({
                url: path,
                method: "POST",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    $("#drop-cluster-button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)

        if (confirm("Are you sure you want to drop cluster " + config.clusterName + "?")) {
            var path = "/admin/" + encodeURIComponent(config.zkAddress) + "/" + config.clusterName
            $("body").css("cursor", "progress")
            $.ajax({
                url: path,
                method: "DELETE",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress)
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    $("#add-instance-button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)
        var clusterName = $(this).attr("cluster")
        var instanceName = $("#instance-name").val()

        if (confirm("Are you sure you want to add instance " + instanceName + " to cluster " + config.clusterName + "?")) {
            var path = "/admin/" + encodeURIComponent(config.zkAddress) + "/" + config.clusterName + "/instances/" + instanceName
            $("body").css("cursor", "progress")
            $.ajax({
                url: path,
                method: "POST",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    $(".enable-instance-button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)
        var clusterName = $(this).attr("cluster")
        var instanceName = $(this).attr("instance")

        if (confirm("Are you sure you want to enable instance " + instanceName + " in cluster " + clusterName + "?")) {
            var path = "/admin/" + encodeURIComponent(config.zkAddress) + "/" + clusterName + "/instances/" + instanceName + "?failIfNoInstance=true"
            $("body").css("cursor", "progress")
            $.ajax({
                url: path,
                method: "POST",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    $(".disable-instance-button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)
        var clusterName = $(this).attr("cluster")
        var instanceName = $(this).attr("instance")

        if (confirm("Are you sure you want to disable instance " + instanceName + " in cluster " + clusterName + "?")) {
            var path = "/admin/" + encodeURIComponent(config.zkAddress) + "/" + clusterName + "/instances/" + instanceName + "?failIfNoInstance=true&disable=true"
            $("body").css("cursor", "progress")
            $.ajax({
                url: path,
                method: "POST",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    $(".drop-instance-button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)
        var clusterName = $(this).attr("cluster")
        var instanceName = $(this).attr("instance")

        if (confirm("Are you sure you want to drop instance " + instanceName + " in cluster " + clusterName + "?")) {
            var path = "/admin/" + encodeURIComponent(config.zkAddress) + "/" + clusterName + "/instances/" + instanceName
            $("body").css("cursor", "progress")
            $.ajax({
                url: path,
                method: "DELETE",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    $("#add-resource-button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)
        var clusterName = $(this).attr("cluster")
        var resourceName = $("#resource-name").val()
        var partitions = $("#resource-partitions").val()
        var replicas = $("#resource-replicas").val()
        var stateModel = $("#resource-state-model").find(":selected").val()
        var rebalanceMode = $("#resource-rebalance-mode").find(":selected").val()

        var url = "/admin/" + encodeURIComponent(config.zkAddress)
            + "/" + clusterName + "/resources/" + resourceName + "/" + partitions + "/" + replicas
            + "?stateModel=" + stateModel + "&rebalanceMode=" + rebalanceMode
            + "&rebalance=true"

        if (confirm("Are you sure you want to add resource " + resourceName + " to cluster " + clusterName + "?")) {
            $("body").css("cursor", "progress")
            $.ajax({
                url: url,
                method: "POST",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    $(".drop-resource-button").click(function(event) {
        event.preventDefault()
        var config = parseDashboardPathname(window.location.pathname)
        var clusterName = $(this).attr("cluster")
        var resourceName = $(this).attr("resource")

        if (confirm("Are you sure you want to drop resource " + resourceName + " in cluster " + clusterName + "?")) {
            var path = "/admin/" + encodeURIComponent(config.zkAddress) + "/" + clusterName + "/resources/" + resourceName
            $("body").css("cursor", "progress")
            $.ajax({
                url: path,
                method: "DELETE",
                success: function(result) {
                    window.pathname = "/dashboard/" + encodeURIComponent(config.zkAddress) + "/" + clusterName
                    window.location.reload()
                },
                error: function(xhr, status, error) {
                    alert(xhr.getResponseHeader("X-Error-Message"))
                    window.location.reload()
                }
            })
        }
    })

    function parseDashboardPathname(pathname) {
        var tokens = pathname.split("/")
        return {
            zkAddress: tokens[2],
            clusterName: tokens[3],
            resourceName: tokens[4]
        }
    }
})
