import {
  Component,
  ElementRef,
  OnInit,
  AfterViewInit,
  OnDestroy
} from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { Observable, Subscription } from 'rxjs';

import * as _ from 'lodash';
import { VisNode, VisNodes, VisEdges, VisNetworkService, VisNetworkData, VisNetworkOptions } from 'ngx-vis';

import { ResourceService } from '../resource/shared/resource.service';
import { InstanceService } from '../instance/shared/instance.service';
import { HelperService } from '../shared/helper.service';

class DashboardNetworkData implements VisNetworkData {
    public nodes: VisNodes;
    public edges: VisEdges;
}

@Component({
  selector: 'hi-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  providers: [ResourceService, InstanceService]
})
export class DashboardComponent implements OnInit, AfterViewInit, OnDestroy {

  visNetwork = 'cluster-dashboard';
  visNetworkData: DashboardNetworkData;
  visNetworkOptions: VisNetworkOptions;

  clusterName: string;
  isLoading = false;
  resourceToId = {};
  instanceToId = {};
  selectedResource: any;
  selectedInstance: any;
  updateSubscription: Subscription;
  updateInterval = 3000;

  constructor(
    private el:ElementRef,
    private route: ActivatedRoute,
    protected visService: VisNetworkService,
    protected resourceService: ResourceService,
    protected instanceService: InstanceService,
    protected helper: HelperService
  ) { }

  networkInitialized() {
    this.visService.on(this.visNetwork, 'click');
    this.visService.on(this.visNetwork, 'zoom');

    this.visService.click
      .subscribe((eventData: any[]) => {
        if (eventData[0] === this.visNetwork) {
          // clear the edges first
          this.visNetworkData.edges.clear();
          this.selectedResource = null;
          this.selectedInstance = null;

          //
          if (eventData[1].nodes.length) {
            const id = eventData[1].nodes[0];
            this.onNodeSelected(id);
          }
        }
      });

    this.visService.zoom
      .subscribe((eventData: any) => {
        if (eventData[0] === this.visNetwork) {
          const scale = eventData[1].scale;
          if (scale == 10) {
            // too big
          } else if (scale < 0.3) {
            // small enough
          }
        }
      });
  }

  ngOnInit() {
    const nodes = new VisNodes();
    const edges = new VisEdges();
    this.visNetworkData = { nodes, edges };

    this.visNetworkOptions = {
      interaction: {
        navigationButtons: true,
        keyboard: true
      },
      layout: {
        // layout will be the same every time the nodes are settled
        randomSeed: 7
      },
      physics: {
        // default is barnesHut
        solver: 'forceAtlas2Based',
        forceAtlas2Based: {
          // default: -50
          gravitationalConstant: -30,
          // default: 0
          // avoidOverlap: 0.3
        }
      },
      groups: {
        resource: {
          color: '#7FCAC3',
          shape: 'ellipse',
          widthConstraint: { maximum: 140 }
        },
        instance: {
          color: '#90CAF9',
          shape: 'box',
          widthConstraint: { maximum: 140 }
        },
        instance_bad: {
          color: '#CA7F86',
          shape: 'box',
          widthConstraint: { maximum: 140 }
        },
        partition: {
          color: '#98D4B1',
          shape: 'ellipse',
          widthConstraint: { maximum: 140 }
        }
      }
    };
  }

  initDashboard() {
    // resize container according to the parent
    let width = this.el.nativeElement.offsetWidth;
    let height = this.el.nativeElement.offsetHeight - 36;
    let dashboardDom = this.el.nativeElement.getElementsByClassName(this.visNetwork)[0];
    dashboardDom.style.width = `${ width }px`;
    dashboardDom.style.height = `${ height }px`;

    // load data
    this.route.parent.params
      .map(p => p.name)
      .subscribe(name => {
        this.clusterName = name;
        this.fetchResources();
        // this.updateResources();
      });
  }

  ngAfterViewInit() {
    setTimeout(_ => this.initDashboard());
  }

  ngOnDestroy(): void {
    if (this.updateSubscription) {
      this.updateSubscription.unsubscribe();
    }

    this.visService.off(this.visNetwork, 'zoom');
    this.visService.off(this.visNetwork, 'click');
  }

  protected fetchResources() {
    this.isLoading = true;

    this.resourceService
      .getAll(this.clusterName)
      .subscribe(
        result => {
          _.forEach(result, (resource) => {
            const newId = this.visNetworkData.nodes.getLength() + 1;
            this.resourceToId[resource.name] = newId;
            this.visNetworkData.nodes.add({
              id: newId,
              label: resource.name,
              group: 'resource',
              title: JSON.stringify(resource)
            });
          });

          this.visService.fit(this.visNetwork);
        },
        error => this.helper.showError(error),
        () => this.isLoading = false
      );

    this.instanceService
      .getAll(this.clusterName)
      .subscribe(
        result => {
          _.forEach(result, (instance) => {
            const newId = this.visNetworkData.nodes.getLength() + 1;
            this.instanceToId[instance.name] = newId;
            this.visNetworkData.nodes.add({
              id: newId,
              label: instance.name,
              group: instance.healthy ? 'instance' : 'instance_bad',
              title: JSON.stringify(instance),
            });
          });

          this.visService.fit(this.visNetwork);
        },
        error => this.helper.showError(error),
        () => this.isLoading = false
      );
  }

  updateResources() {
    /* disable auto-update for now
    this.updateSubscription = Observable
      .interval(this.updateInterval)
      .flatMap(i => this.instanceService.getAll(this.clusterName))*/
    this.instanceService.getAll(this.clusterName)
      .subscribe(
        result => {
          _.forEach(result, instance => {
            const id = this.instanceToId[instance.name];
            this.visNetworkData.nodes.update([{
              id: id,
              group: instance.healthy ? 'instance' : 'instance_bad'
            }]);
          });
        }
      );
  }

  protected onNodeSelected(id) {
    const instanceName = _.findKey(this.instanceToId, value => value === id);
    if (instanceName) {
      this.selectedInstance = instanceName;
      // fetch relationships
      this.resourceService
        .getAllOnInstance(this.clusterName, instanceName)
        .subscribe(
          resources => {
            _.forEach(resources, (resource) => {
              this.visNetworkData.edges.add({
                from: id,
                to: this.resourceToId[resource.name]
              });
            });
          },
          error => this.helper.showError(error)
        );
    } else {
      const resourceName = _.findKey(this.resourceToId, value => value === id);
      if (resourceName) {
        this.selectedResource = resourceName;
        this.resourceService
          .get(this.clusterName, resourceName)
          .subscribe(
            resource => {
              _(resource.partitions)
                .flatMap('replicas')
                .unionBy('instanceName')
                .map('instanceName')
                .forEach((instanceName) => {
                  this.visNetworkData.edges.add({
                    from: this.instanceToId[instanceName],
                    to: this.resourceToId[resourceName]
                  });
                });
            },
            error => this.helper.showError(error)
          );
      }
    }
  }

}
