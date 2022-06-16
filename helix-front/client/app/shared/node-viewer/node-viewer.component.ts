import {
  Component,
  OnInit,
  Input,
  Output,
  ViewChild,
  ViewEncapsulation,
  EventEmitter,
} from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';

import * as _ from 'lodash';
// import * as ace from 'ace-builds/src-noconflict/ace';
import 'ace-builds';
import 'ace-builds/webpack-resolver';
import 'ace-builds/src-noconflict/mode-json';
import 'ace-builds/src-noconflict/theme-chrome';
import { config } from 'ace-builds';

import { Node } from '../models/node.model';
import { Settings } from '../../core/settings';
import { InputDialogComponent } from '../dialog/input-dialog/input-dialog.component';
import { ConfirmDialogComponent } from '../dialog/confirm-dialog/confirm-dialog.component';

config.set(
  'basePath',
  'https://cdn.jsdelivr.net/npm/ace-builds@1.6.0/src-noconflict/'
);
config.setModuleUrl(
  'ace/mode/javascript_worker',
  'https://cdn.jsdelivr.net/npm/ace-builds@1.6.0/src-noconflict/worker-json.js'
);
@Component({
  selector: 'hi-node-viewer',
  templateUrl: './node-viewer.component.html',
  styleUrls: ['./node-viewer.component.scss'],
  // Since we are importing external styles in this component
  // we will not use Shadow DOM at all to make sure the styles apply
  encapsulation: ViewEncapsulation.None,
})
export class NodeViewerComponent implements OnInit {
  @ViewChild('simpleTable', { static: true }) simpleTable;
  @ViewChild('listTable', { static: true }) listTable;
  @ViewChild('mapTable', { static: true }) mapTable;
  // @ViewChild('editor') editor;
  // text: string = '';

  // ngAfterViewInit() {
  //   // this.editor.setTheme('chrome');

  //   this.editor.getEditor().setOptions({
  //     enableBasicAutocompletion: false,
  //     readOnly: true,
  //   });

  //   this.editor.getEditor().commands.addCommand({
  //     name: 'showOtherCompletions',
  //     bindKey: 'Ctrl-.',
  //     exec: function (editor) {},
  //   });
  // }

  @Output('update')
  change: EventEmitter<Node> = new EventEmitter<Node>();

  @Output('create')
  create: EventEmitter<Node> = new EventEmitter<Node>();

  @Output('delete')
  delete: EventEmitter<Node> = new EventEmitter<Node>();

  @Input()
  unlockable = false;

  @Input()
  loadingIndicator = false;

  private _editable = false;

  protected _obj: any;
  protected node: Node;

  headerHeight = Settings.tableHeaderHeight;
  rowHeight = Settings.tableRowHeight;
  sorts = [{ prop: 'name', dir: 'asc' }];
  keyword = '';
  columns = {
    simpleConfigs: [
      {
        name: 'Name',
        editable: false,
      },
      {
        name: 'Value',
        editable: false,
      },
    ],
    listConfigs: [
      {
        name: 'Value',
        editable: false,
      },
    ],
  };

  _simpleConfigs: any[];

  _listConfigs: any[];

  _mapConfigs: any[];

  // MODE 1: use directly in components
  @Input()
  set obj(value: any) {
    if (value != null) {
      this._obj = value;
      this.node = new Node(value);
    }
  }
  get obj(): any {
    return this._obj;
  }
  set objString(value: string) {
    if (value !== null) {
      let parsedValue = JSON.parse('');
      if (value) {
        try {
          JSON.parse(value);
        } catch (err) {
          console.log(err);
        }
      }
      this.obj = parsedValue;
      this.node = new Node(parsedValue);
    }
  }
  get objString(): string {
    return JSON.stringify(this.obj, null, 2);
  }
  set editable(value: boolean) {
    this._editable = value;
    this.columns.simpleConfigs[1].editable = this._editable;
    this.columns.listConfigs[0].editable = this._editable;
  }
  get editable() {
    return this._editable;
  }
  get simpleConfigs(): any[] {
    return this.node
      ? _.filter(
          this.node.simpleFields,
          (config) =>
            config.name.toLowerCase().indexOf(this.keyword) >= 0 ||
            config.value.toLowerCase().indexOf(this.keyword) >= 0
        )
      : [];
  }
  get listConfigs(): any[] {
    return this.node
      ? _.filter(
          this.node.listFields,
          (config) =>
            config.name.toLowerCase().indexOf(this.keyword) >= 0 ||
            _.some(
              config.value as any[],
              (subconfig) =>
                subconfig.value.toLowerCase().indexOf(this.keyword) >= 0
            )
        )
      : [];
  }
  get mapConfigs(): any[] {
    return this.node
      ? _.filter(
          this.node.mapFields,
          (config) =>
            config.name.toLowerCase().indexOf(this.keyword) >= 0 ||
            _.some(
              config.value as any[],
              (subconfig) =>
                subconfig.name.toLowerCase().indexOf(this.keyword) >= 0 ||
                subconfig.value.toLowerCase().indexOf(this.keyword) >= 0
            )
        )
      : [];
  }

  constructor(protected dialog: MatDialog, protected route: ActivatedRoute) {}

  ngOnInit() {
    // MODE 2: use in router
    if (this.route.snapshot.data.path) {
      const path = this.route.snapshot.data.path;

      // try parent data first
      this.obj = _.get(this.route.parent, `snapshot.data.${path}`);

      if (this.obj == null) {
        // try self data then
        this.obj = _.get(this.route.snapshot.data, path);
      }

      this.objString = JSON.stringify(this.obj, null, 2);
    }
  }

  updateFilter(event) {
    this.keyword = event.target.value.toLowerCase().trim();

    // Whenever the filter changes, always go back to the first page
    if (this.simpleTable) {
      this.simpleTable.offset = 0;
    }
    if (this.listTable) {
      this.listTable.offset = 0;
    }
    if (this.mapTable) {
      this.mapTable.offset = 0;
    }
  }

  getNameCellClass({ value }): any {
    return {
      // highlight HELIX own configs
      primary: _.snakeCase(value).toUpperCase() === value,
    };
  }

  onCreate(type) {
    this.dialog
      .open(InputDialogComponent, {
        data: {
          title: `Create a new ${type} configuration`,
          message:
            "Please enter the name of the new configuration. You'll be able to add values later:",
          values: {
            name: {
              label: 'the name of the new configuration',
            },
          },
        },
      })
      .afterClosed()
      .subscribe((result) => {
        if (result) {
          const entry = [
            {
              name: result.name.value,
              value: [],
            },
          ];

          const newNode: Node = new Node(null);
          if (type === 'list') {
            newNode.listFields = entry;
          } else if (type === 'map') {
            newNode.mapFields = entry;
          }

          this.create.emit(newNode);
        }
      });
  }

  beforeDelete(type, row) {
    this.dialog
      .open(ConfirmDialogComponent, {
        data: {
          title: 'Confirmation',
          message: 'Are you sure you want to delete this configuration?',
        },
      })
      .afterClosed()
      .subscribe((result) => {
        if (result) {
          this.onDelete(type, row);
        }
      });
  }

  onDelete(type, row) {
    const newNode: Node = new Node(null);

    if (type === 'simple') {
      newNode.appendSimpleField(row.name, '');
    } else if (type === 'list') {
      newNode.listFields = [{ name: row.name, value: [] }];
    } else if (type === 'map') {
      newNode.mapFields = [{ name: row.name, value: null }];
    }

    this.delete.emit(newNode);
  }

  created(type, data, key) {
    const newNode: Node = new Node(null);

    switch (type) {
      case 'simple':
        newNode.appendSimpleField(data.name.value, data.value.value);
        break;

      case 'list':
        if (key) {
          const entry = _.find(this.node.listFields, { name: key });
          //         Property 'value' does not exist on type 'number | ListFieldObject | ((searchElement: ListFieldObject, fromIndex?: number) => boolean) | ((...'.
          // Property 'value' does not exist on type 'number'.ts(2339)
          // @ts-ignore
          entry.value.push({
            name: '',
            value: data.value.value,
          });
          // Argument of type 'number | ListFieldObject | ((searchElement: ListFieldObject, fromIndex?: number) => boolean) | ((...' is not assignable to parameter of type 'ListFieldObject'.
          // Type 'number' is not assignable to type 'ListFieldObject'.ts(2345)
          // @ts-ignore
          newNode.listFields.push(entry);
        }
        break;

      case 'map':
        if (key) {
          const entry = _.find(this.node.mapFields, { name: key });
          //         Property 'value' does not exist on type 'number | MapFieldObject | ((searchElement: MapFieldObject, fromIndex?: number) => boolean) | (() ...'.
          // Property 'value' does not exist on type 'number'.ts(2339)
          // @ts-ignore
          _.forEach(entry.value, (item: any) => {
            newNode.appendMapField(key, item.name, item.value);
          });
          newNode.appendMapField(key, data.name.value, data.value.value);
        }
        break;
    }

    this.create.emit(newNode);
  }

  edited(type, { row, column, value }, key, isDeleting) {
    if (!isDeleting && column.name !== 'Value') {
      return;
    }

    const newNode: Node = new Node(null);

    switch (type) {
      case 'simple':
        newNode.appendSimpleField(row.name, value);
        break;

      case 'list':
        if (key) {
          const entry = _.find(this.node.listFields, { name: key });
          //         Property 'value' does not exist on type 'number | ListFieldObject | ((searchElement: ListFieldObject, fromIndex?: number) => boolean) | ((...'.
          // Property 'value' does not exist on type 'number'.ts(2339)
          // @ts-ignore
          const index = _.findIndex(entry.value, { value: row.value });
          if (isDeleting) {
            //           Property 'value' does not exist on type 'number | ListFieldObject | ((searchElement: ListFieldObject, fromIndex?: number) => boolean) | ((...'.
            // Property 'value' does not exist on type 'number'.ts(2339)
            // @ts-ignore
            entry.value.splice(index, 1);
          } else {
            //           Property 'value' does not exist on type 'number | ListFieldObject | ((searchElement: ListFieldObject, fromIndex?: number) => boolean) | ((...'.
            // Property 'value' does not exist on type 'number'.ts(2339)
            // @ts-ignore
            entry.value[index].value = value;
          }
          // Argument of type 'number | ListFieldObject | ((searchElement: ListFieldObject, fromIndex?: number) => boolean) | ((...' is not assignable to parameter of type 'ListFieldObject'.
          // Type 'number' is not assignable to type 'ListFieldObject'.ts(2345)
          // @ts-ignore
          newNode.listFields.push(entry);
        }
        break;

      case 'map':
        if (key) {
          // have to fetch all other configs under this key
          const entry = _.find(this.node.mapFields, { name: key });
          newNode.mapFields = [{ name: key, value: [] }];

          // Property 'value' does not exist on type 'number | MapFieldObject | ((searchElement: MapFieldObject, fromIndex?: number) => boolean) | (() ...'.
          //   Property 'value' does not exist on type 'number'.ts(2339)
          // @ts-ignore
          _.forEach(entry.value, (item: any) => {
            if (item.name === row.name) {
              if (!isDeleting) {
                newNode.appendMapField(key, item.name, value);
              }
            } else {
              newNode.appendMapField(key, item.name, item.value);
            }
          });
        }
        break;
    }

    this.change.emit(newNode);
  }
}
