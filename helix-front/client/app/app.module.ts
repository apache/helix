import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';

import { Angulartics2Module, Angulartics2Piwik } from 'angulartics2';

import { AppRoutingModule } from './app-routing.module';
import { CoreModule } from './core/core.module';
import { SharedModule } from './shared/shared.module';
import { ClusterModule } from './cluster/cluster.module';
import { ConfigurationModule } from './configuration/configuration.module';
import { InstanceModule } from './instance/instance.module';
import { ResourceModule } from './resource/resource.module';
import { ControllerModule } from './controller/controller.module';
import { HistoryModule } from './history/history.module';
import { AppComponent } from './app.component';
import { WorkflowModule } from './workflow/workflow.module';
import { ChooserModule } from './chooser/chooser.module';
import { DashboardModule } from './dashboard/dashboard.module';

@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,
    AppRoutingModule,
    Angulartics2Module.forRoot([ Angulartics2Piwik ]),
    CoreModule,
    SharedModule,
    ClusterModule,
    ConfigurationModule,
    InstanceModule,
    ResourceModule,
    ControllerModule,
    HistoryModule,
    WorkflowModule,
    ChooserModule,
    DashboardModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
