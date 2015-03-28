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

    var uri = $("#visualizer-data-uri").text()

    if (uri) {
        var diameter = 960,
            format = d3.format(",d");

        var color = d3.scale.category20();

        var pack = d3.layout.pack()
            .size([diameter - 4, diameter - 4])
            .value(function(d) { return d.size; });

        var svg = d3.select("#resource-visualizer").append("svg")
            .attr("width", diameter)
            .attr("height", diameter)
            .append("g")
            .attr("transform", "translate(2,2)");

        var loading = svg.append("text")
            .attr("x", diameter / 2)
            .attr("y", diameter / 4)
            .attr("dy", ".35em")
            .style("text-anchor", "middle")
            .text("Rendering...");

        /** Some fixed state colors */
        var stateColors = {
            "MASTER": "#405898",
            "SLAVE": "#8c9ac1",
            "OFFLINE": "#c5c5c5",
            "ONLINE": "#92ca4e",
            "STANDBY": "#c2c0c3",
            "LEADER": "#ffd700",
            "ERROR": "#ff002d",
            "DEAD": "#FF9494",
            "DISABLED": "#FFFF94",
            "LIVE": "#C7FAB1",
            "": "#F0F2FF",
            "N/A": "#DF7F4D"
        }

        d3.json(uri, function(error, root) {
          var node = svg.datum(root).selectAll(".node")
              .data(pack.nodes)
              .enter().append("g")
              .attr("class", function(d) { return d.children ? "node" : "leaf node"; })
              .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

          node.append("title")
              .text(function(d) {
                    var title = d.name
                    if (!d.children) {
                        title += " (" + d.parentName + ")"
                    }
                    if (d.state) {
                        title += ": " + d.state
                    }
                    return title
               });

          node.append("circle")
              .attr("r", function(d) { return d.r; })
              .style("fill", function(d) {
                    if (stateColors[d.state]) {
                        return stateColors[d.state]
                    } else {
                        return color(d.state) // random color
                    }
              });

          node.filter(function(d) { return !d.children; }).append("text")
              .attr("dy", ".3em")
              .style("text-anchor", "middle")
              .text(function(d) {
                    var suffix = d.name.substring(d.name.lastIndexOf("_") + 1)
                    var id = new Number(suffix)
                    if (isNaN(id)) {
                      return d.name.substring(0, d.r / 3);
                    }
                    return id
               });

          loading.remove()
        });

        d3.select(self.frameElement).style("height", diameter + "px");
    }
})
