import { Component, OnInit, Input, ElementRef, AfterViewInit, ViewChild } from '@angular/core';

import * as shape from 'd3-shape';
import * as _ from 'lodash';

import { Workflow, Job } from '../shared/workflow.model';

@Component({
  selector: 'hi-workflow-dag',
  templateUrl: './workflow-dag.component.html',
  styleUrls: ['./workflow-dag.component.scss']
})
export class WorkflowDagComponent implements OnInit, AfterViewInit {

  @Input()
  workflow: Workflow;
  curve: any = shape.curveLinear;
  view = [800, 600];
  data = {
    nodes: [],
    links: []
  };
  jobNameToId = {};

  @ViewChild('graph')
  graph;

  constructor(protected el:ElementRef) { }

  ngOnInit() {
    this.loadJobs();
  }

  ngAfterViewInit() {
    // console.log(this.el);
    // console.log(this.graph);
  }

  protected loadJobs() {
    // process nodes
    _.forEach(this.workflow.jobs, (job: Job) => {
      const newId = (this.data.nodes.length + 1).toString();

      this.data.nodes.push({
        id: newId,
        label: job.name,
        description: job.rawName,
        state: job.state
      });
      this.jobNameToId[job.name] = newId;
    });

    // process edges/links
    _.forEach(this.workflow.jobs, (job: Job) => {
      _.forEach(job.parents, parentName => {
        this.data.links.push({
          source: this.jobNameToId[parentName],
          target: this.jobNameToId[job.name]
        });
      });
    });
  }
}
