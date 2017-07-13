import { ModuleWithProviders } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { AppComponent } from './app.component';
import { NodeViewerComponent } from './shared/node-viewer/node-viewer.component';
import { ClusterResolver } from './cluster/shared/cluster.resolver';
import { ClusterDetailComponent } from './cluster/cluster-detail/cluster-detail.component';
import { ConfigDetailComponent } from './configuration/config-detail/config-detail.component';
import { InstanceListComponent } from './instance/instance-list/instance-list.component';
import { ResourceResolver } from './resource/shared/resource.resolver';
import { ResourceListComponent } from './resource/resource-list/resource-list.component';
import { ResourceDetailComponent } from './resource/resource-detail/resource-detail.component';
import { PartitionListComponent } from './resource/partition-list/partition-list.component';
import { ControllerDetailComponent } from './controller/controller-detail/controller-detail.component';
import { HistoryListComponent } from './history/history-list/history-list.component';
import { InstanceDetailComponent } from './instance/instance-detail/instance-detail.component';

const HELIX_ROUTES: Routes = [
  {
    path: '',
    redirectTo: '/clusters',
    pathMatch: 'full'
  },
  {
    path: 'embed',
    redirectTo: '/clusters?embed=true',
    pathMatch: 'full'
  },
  {
    path: 'clusters',
    // TODO vxu: in future hook this route with an introduction or welcome page
    component: ClusterDetailComponent
  },
  {
    path: 'clusters/:name',
    component: ClusterDetailComponent,
    resolve: {
      cluster: ClusterResolver
    },
    children: [
      {
        path: '',
        redirectTo: 'resources',
        pathMatch: 'full'
      },
      {
        path: 'configs',
        component: ConfigDetailComponent,
        data: {
          forCluster: true
        }
      },
      {
        path: 'instances',
        component: InstanceListComponent
      },
      {
        path: 'resources',
        component: ResourceListComponent
      }
    ]
  },
  {
    path: 'clusters/:cluster_name/controller',
    component: ControllerDetailComponent,
    children: [
      {
        path: '',
        redirectTo: 'history',
        pathMatch: 'full'
      },
      {
        path: 'history',
        component: HistoryListComponent
      }
    ]
  },
  {
    path: 'clusters/:cluster_name/resources/:resource_name',
    component: ResourceDetailComponent,
    resolve: {
      resource: ResourceResolver
    },
    children: [
      {
        path: '',
        redirectTo: 'partitions',
        pathMatch: 'full'
      },
      {
        path: 'partitions',
        component: PartitionListComponent
      },
      {
        path: 'externalView',
        component: NodeViewerComponent,
        data: {
          path: 'resource.externalView'
        }
      },
      {
        path: 'idealState',
        component: NodeViewerComponent,
        data: {
          path: 'resource.idealState'
        }
      },
      {
        path: 'configs',
        component: ConfigDetailComponent,
        data: {
          forResource: true
        }
      }
    ]
  },
  {
    path: 'clusters/:cluster_name/instances/:instance_name',
    component: InstanceDetailComponent,
    children: [
      {
        path: '',
        redirectTo: 'resources',
        pathMatch: 'full'
      },
      {
        path: 'resources',
        component: ResourceListComponent,
        data: {
          forInstance: true
        }
      },
      {
        path: 'configs',
        component: ConfigDetailComponent,
        data: {
          forInstance: true
        }
      },
      {
        path: 'history',
        component: HistoryListComponent
      }
    ]
  }
];

export const AppRoutingModule: ModuleWithProviders = RouterModule.forRoot(HELIX_ROUTES);
