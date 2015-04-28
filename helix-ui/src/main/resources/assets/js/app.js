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
    var hashRoute = getHashRoute()

    if (hashRoute["switcher"]) {
        var active = parseInt(hashRoute["switcher"])

        $("#switcher-tabs > li > a").each(function(i, elt) {
            if (active === i) {
                $(elt).trigger("click")
            }
        })
    }

    $("#switcher-tabs > li > a").click(function(event) {
        var active = 0
        var text = $(this).text()
        $("#switcher-tabs > li").each(function(i, elt) {
            if ($(elt).find("a").text() === text) {
                active = i
            }
        })

        var hashRoute = getHashRoute()
        hashRoute["switcher"] = active
        window.location.hash = encodeHashRoute(hashRoute)
    })
})

/**
 * @return
 *  true if the row's column values has the same prefix as any in the filter (i.e. OR)
 */
function applyFilter(filter, row) {
    var passingCells = []
    for (var i = 0; i < filter.length; i++) {
        passingCells.push(true)
    }

    $(row).find("td").each(function(j, column) {
        var columnFilter = filter[j]
        if (columnFilter.length > 0) {
            var passesOne = false
            $.each(columnFilter, function(k, filterValue) {
                if ($(column).text().indexOf(filterValue) === 0) {
                    passesOne = true
                }
            })

            if (!passesOne) {
                //passesAll = false
                passingCells[j] = false
            }
        }
    })

    // Must pass all
    for (var i = 0; i < passingCells.length; i++) {
        if (!passingCells[i]) {
            return false
        }
    }
    return true
}

function filterTable(filterFormId, tableId) {
    var filter = [[], [], [], []]

    $(filterFormId + " > div").each(function(i, row) {
        var filterPosition = $(row).find("select").find(":selected").val()
        var filterValue = $(row).find("input").val()
        filter[filterPosition].push(filterValue)
    })

    $(tableId + " > tbody > tr").each(function(i, row) {
        if (applyFilter(filter, row)) {
            $(row).css('display', 'table-row')
        } else {
            $(row).css('display', 'none')
        }
    })
}

function getHashRoute() {
    var hashRoute = {}

    if (window.location.hash.length > 1) {
        var tokens = window.location.hash.substring(1).split("&")
        $.each(tokens, function(i, token) {
            var keyValue = token.split("=")
            hashRoute[keyValue[0]] = keyValue[1]
        })
    }

    return hashRoute
}

function encodeHashRoute(hashRoute) {
    var tokens = []
    $.each(hashRoute, function(key, value) {
        tokens.push(key + "=" + value)
    })
    return "#" + tokens.join("&")
}
