import { ModuleWithProviders } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { AppComponent } from './app.component';
import { ClusterResolver } from './cluster/shared/cluster.resolver';
import { ClusterDetailComponent } from './cluster/cluster-detail/cluster-detail.component';
import { ConfigDetailComponent } from './configuration/config-detail/config-detail.component';
import { InstanceListComponent } from './instance/instance-list/instance-list.component';
import { ResourceResolver } from './resource/shared/resource.resolver';
import { ResourceListComponent } from './resource/resource-list/resource-list.component';
import { ResourceDetailComponent } from './resource/resource-detail/resource-detail.component';
import { ControllerDetailComponent } from './controller/controller-detail/controller-detail.component';
import { HistoryListComponent } from './history/history-list/history-list.component';

const HELIX_ROUTES: Routes = [
  {
    path: '',
    redirectTo: '/clusters',
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
        path: 'config',
        component: ConfigDetailComponent
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
    path: 'clusters/:name/controller',
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
    }
  }
];

export const AppRoutingModule: ModuleWithProviders = RouterModule.forRoot(HELIX_ROUTES);
