import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { FlexLayoutModule } from '@angular/flex-layout';
import { MaterialModule } from '@angular/material';

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

@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,
    AppRoutingModule,
    CoreModule,
    SharedModule,
    FlexLayoutModule,
    MaterialModule,
    ClusterModule,
    ConfigurationModule,
    InstanceModule,
    ResourceModule,
    ControllerModule,
    HistoryModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
