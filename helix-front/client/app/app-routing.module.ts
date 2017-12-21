import { ModuleWithProviders } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { AppComponent } from './app.component';
import { ClusterComponent } from './cluster/cluster.component';
import { ClusterDetailComponent } from './cluster/cluster-detail/cluster-detail.component';
import { ConfigDetailComponent } from './configuration/config-detail/config-detail.component';
import { InstanceListComponent } from './instance/instance-list/instance-list.component';
import { ResourceListComponent } from './resource/resource-list/resource-list.component';
import { ResourceDetailComponent } from './resource/resource-detail/resource-detail.component';
import { ResourceNodeViewerComponent } from './resource/resource-node-viewer/resource-node-viewer.component';
import { PartitionListComponent } from './resource/partition-list/partition-list.component';
import { ControllerDetailComponent } from './controller/controller-detail/controller-detail.component';
import { HistoryListComponent } from './history/history-list/history-list.component';
import { InstanceDetailComponent } from './instance/instance-detail/instance-detail.component';
import { WorkflowListComponent } from './workflow/workflow-list/workflow-list.component';
import { WorkflowDetailComponent } from './workflow/workflow-detail/workflow-detail.component';
import { HelixListComponent } from './chooser/helix-list/helix-list.component';
import { DashboardComponent } from './dashboard/dashboard.component';

const HELIX_ROUTES: Routes = [
  {
    path: '',
    component: HelixListComponent
  },
  {
    path: 'embed',
    redirectTo: '/?embed=true',
    pathMatch: 'full'
  },
  {
    path: ':helix',
    component: ClusterComponent,
    children: [
      {
        path: ':name',
        component: ClusterDetailComponent,
        children: [
          {
            path: '',
            redirectTo: 'resources',
            pathMatch: 'full'
          },
          {
            path: 'configs',
            component: ConfigDetailComponent
          },
          {
            path: 'instances',
            component: InstanceListComponent
          },
          {
            path: 'resources',
            component: ResourceListComponent
          },
          {
            path: 'workflows',
            component: WorkflowListComponent
          },
          {
            path: 'dashboard',
            component: DashboardComponent
          }
        ]
      },
      {
        path: ':cluster_name/controller',
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
        path: ':cluster_name/resources/:resource_name',
        component: ResourceDetailComponent,
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
            component: ResourceNodeViewerComponent,
            data: {
              path: 'externalView'
            }
          },
          {
            path: 'idealState',
            component: ResourceNodeViewerComponent,
            data: {
              path: 'idealState'
            }
          },
          {
            path: 'configs',
            component: ConfigDetailComponent
          }
        ]
      },
      {
        path: ':cluster_name/workflows/:workflow_name',
        component: WorkflowDetailComponent
      },
      {
        path: ':cluster_name/instances/:instance_name',
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
            component: ConfigDetailComponent
          },
          {
            path: 'history',
            component: HistoryListComponent
          }
        ]
      }
    ]
  }
];

export const AppRoutingModule: ModuleWithProviders = RouterModule.forRoot(HELIX_ROUTES);
