import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';

import { Angulartics2Module, Angulartics2Matomo } from 'angulartics2';

import { appRoutingModule } from './app-routing.module';
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
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule,
    appRoutingModule,
    // Type '(typeof Angulartics2Matomo)[]'
    // has no properties in common with type 'Partial<Angulartics2Settings>'.ts(2559)
    // @ts-expect-error
    Angulartics2Module.forRoot([ Angulartics2Matomo ]),
    CoreModule,
    ClusterModule,
    ConfigurationModule,
    InstanceModule,
    ResourceModule,
    ControllerModule,
    HistoryModule,
    ChooserModule,
    DashboardModule,
    BrowserAnimationsModule,
  ],
  providers: [SharedModule, WorkflowModule],
  bootstrap: [AppComponent]
})
export class AppModule { }
