import {
  Component,
  ElementRef,
  OnInit,
  AfterViewInit,
  OnDestroy,
} from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { map } from 'rxjs/operators';

import _ from 'lodash';
import { forEach as lodashForEach } from 'lodash';
import { Data, Edge, Node, Options, VisNetworkService } from 'ngx-vis';

import { ResourceService } from '../resource/shared/resource.service';
import { InstanceService } from '../instance/shared/instance.service';
import { HelperService } from '../shared/helper.service';
@Component({
  selector: 'hi-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  providers: [ResourceService, InstanceService],
})
export class DashboardComponent implements OnInit, AfterViewInit, OnDestroy {
  public visNetwork = 'cluster-dashboard';
  public visNetworkData: Data;
  public visNetworkOptions: Options;
  public nodes: Node[];
  public edges: Edge[];

  public clusterName: string;
  public isLoading = false;
  public resourceToId = {};
  public instanceToId = {};
  public selectedResource: any;
  public selectedInstance: any;
  public updateSubscription: Subscription;
  public updateInterval = 3000;

  public constructor(
    private el: ElementRef,
    private route: ActivatedRoute,
    protected visNetworkService: VisNetworkService,
    protected resourceService: ResourceService,
    protected instanceService: InstanceService,
    protected helper: HelperService
  ) {}

  networkInitialized() {
    this.visNetworkService.on(this.visNetwork, 'click');
    this.visNetworkService.on(this.visNetwork, 'zoom');

    this.visNetworkService.click.subscribe((eventData: any[]) => {
      if (eventData[0] === this.visNetwork) {
        // clear the edges
        this.visNetworkData.edges = [];
        this.selectedResource = null;
        this.selectedInstance = null;

        //
        if (eventData[1].nodes.length) {
          const id = eventData[1].nodes[0];
          this.onNodeSelected(id);
        }
      }
    });

    this.visNetworkService.zoom.subscribe((eventData: any) => {
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
    this.nodes = [];
    this.edges = [];
    this.visNetworkData = { nodes: this.nodes, edges: this.edges };
    this.visNetworkOptions = {
      interaction: {
        navigationButtons: true,
        keyboard: true,
      },
      layout: {
        // layout will be the same every time the nodes are settled
        randomSeed: 7,
      },
      physics: {
        // default is barnesHut
        solver: 'forceAtlas2Based',
        forceAtlas2Based: {
          // default: -50
          gravitationalConstant: -30,
          // default: 0
          // avoidOverlap: 0.3
        },
      },
      groups: {
        resource: {
          color: '#7FCAC3',
          shape: 'ellipse',
          widthConstraint: { maximum: 140 },
        },
        instance: {
          color: '#90CAF9',
          shape: 'box',
          widthConstraint: { maximum: 140 },
        },
        instance_bad: {
          color: '#CA7F86',
          shape: 'box',
          widthConstraint: { maximum: 140 },
        },
        partition: {
          color: '#98D4B1',
          shape: 'ellipse',
          widthConstraint: { maximum: 140 },
        },
      },
    };
  }

  initDashboard() {
    // resize container according to the parent
    const width = this.el.nativeElement.offsetWidth;
    const height = this.el.nativeElement.offsetHeight - 36;
    const dashboardDom = this.el.nativeElement.getElementsByClassName(
      this.visNetwork
    )[0];
    dashboardDom.style.width = `${width}px`;
    dashboardDom.style.height = `${height}px`;

    // load data
    if (this.route && this.route.parent) {
      this.route.parent.params.pipe(map((p) => p.name)).subscribe((name) => {
        this.clusterName = name;
        this.fetchResources();
        // this.updateResources();
      });
    }
  }

  ngAfterViewInit() {
    setTimeout((_) => this.initDashboard());
  }

  ngOnDestroy(): void {
    if (this.updateSubscription) {
      this.updateSubscription.unsubscribe();
    }

    this.visNetworkService.off(this.visNetwork, 'zoom');
    this.visNetworkService.off(this.visNetwork, 'click');
  }

  protected fetchResources() {
    this.isLoading = true;

    this.resourceService.getAll(this.clusterName).subscribe(
      (result) => {
        lodashForEach(result, (resource) => {
          const lastId = this.nodes.length;
          const newId = this.nodes.length + 1;
          this.resourceToId[resource.name] = newId;
          (this.visNetworkData.nodes as Node[])[
            this.visNetworkData.nodes.length
          ] = {
            id: newId,
            label: resource.name,
            group: 'resource',
            title: JSON.stringify(resource),
          };
        });

        this.visNetworkService.fit(this.visNetwork);
      },
      (error) => this.helper.showError(error),
      () => (this.isLoading = false)
    );

    this.instanceService.getAll(this.clusterName).subscribe(
      (result) => {
        lodashForEach(result, (instance) => {
          const newId = this.visNetworkData.nodes.length + 1;
          this.instanceToId[instance.name] = newId;
          (this.visNetworkData.nodes as Node[])[
            this.visNetworkData.nodes.length
          ] = {
            id: newId,
            label: instance.name,
            group: instance.healthy ? 'instance' : 'instance_bad',
            title: JSON.stringify(instance),
          };
        });

        this.visNetworkService.fit(this.visNetwork);
      },
      (error) => this.helper.showError(error),
      () => (this.isLoading = false)
    );
  }

  updateResources() {
    /* disable auto-update for now
    this.updateSubscription = Observable
      .interval(this.updateInterval)
      .flatMap(i => this.instanceService.getAll(this.clusterName))*/
    this.instanceService.getAll(this.clusterName).subscribe((result) => {
      _.forEach(result, (instance, index, _collection) => {
        (this.visNetworkData.nodes as Node[])[index] = {
          id: this.instanceToId[instance.name],
          group: instance.healthy ? 'instance' : 'instance_bad',
        };
      });
    });
  }

  protected onNodeSelected(id) {
    const instanceName = _.findKey(this.instanceToId, (value) => value === id);
    if (instanceName) {
      this.selectedInstance = instanceName;
      // fetch relationships
      this.resourceService
        .getAllOnInstance(this.clusterName, instanceName)
        .subscribe(
          (resources) => {
            lodashForEach(resources, (resource) => {
              (this.visNetworkData.edges as Edge[])[
                this.visNetworkData.nodes.length
              ] = {
                from: id,
                to: this.resourceToId[resource.name],
              };
            });
          },
          (error) => this.helper.showError(error)
        );
    } else {
      const resourceName = _.findKey(
        this.resourceToId,
        (value) => value === id
      );
      if (resourceName) {
        this.selectedResource = resourceName;
        this.resourceService.get(this.clusterName, resourceName).subscribe(
          (resource) => {
            _(resource.partitions)
              .flatMap('replicas')
              .unionBy('instanceName')
              .map('instanceName')
              .forEach((instanceName) => {
                (this.visNetworkData.edges as Edge[])[
                  this.visNetworkData.nodes.length
                ] = {
                  from: this.instanceToId[instanceName],
                  to: this.resourceToId[resourceName],
                };
              });
          },
          (error) => this.helper.showError(error)
        );
      }
    }
  }
}
