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

    // Don't submit when enter is pressed anywhere on the page
    $(window).keydown(function(event){
        if(event.keyCode == 13) {
          event.preventDefault()
          return false
        }
    })

    $('#filter-add').click(function(event) {
        event.preventDefault()

        var div = $('<div></div>', {
            class: 'uk-form-row'
        })

        var select = $('<select></select>')
            .append($('<option></option>', {
                text: 'Partition',
                value: 0
            }))
            .append($('<option></option>', {
                text: 'Instance',
                value: 1
            }))
            .append($('<option></option>', {
                text: 'Ideal',
                value: 2
            }))
            .append($('<option></option>', {
                text: 'External',
                value: 3
            }))

        var input = $('<input></input>', {
            type: 'text'
        }).keyup(function() {
            filterTable("#filter-form", "#resource-state-table")
        })

        var closeButton = $('<a></a>', {
            class: 'uk-close'
        }).click(function() {
            div.remove()
            filterTable("#filter-form", "#resource-state-table")
        })

        div.append(select)
           .append(input)
           .append(closeButton)

        $("#filter-form").append(div)
    })
})
