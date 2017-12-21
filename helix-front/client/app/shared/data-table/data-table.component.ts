import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { MatDialog } from '@angular/material';

import { Settings } from '../../core/settings';
import { InputDialogComponent } from '../dialog/input-dialog/input-dialog.component';
import { ConfirmDialogComponent } from '../dialog/confirm-dialog/confirm-dialog.component';

@Component({
  selector: 'hi-data-table',
  templateUrl: './data-table.component.html',
  styleUrls: ['./data-table.component.scss']
})
export class DataTableComponent implements OnInit {

  @Input() rows = [];
  @Input() columns = [];
  @Input() sorts = [];
  @Input() deletable = false;
  @Input() insertable = false;

  @Output() update: EventEmitter<any> = new EventEmitter<any>();
  @Output() create: EventEmitter<any> = new EventEmitter<any>();
  @Output() delete: EventEmitter<any> = new EventEmitter<any>();

  rowHeight = Settings.tableRowHeight;

  constructor(
    protected dialog: MatDialog
  ) { }

  ngOnInit() {
  }

  onEdited(row, column, value) {
    const prop = this.getPropName(column);

    // only emit when value changes
    if (row[prop] !== value) {
      this.update.emit({
        row: row,
        column: column,
        value: value
      });
    }
  }

  onCreate() {
    let data = {
      title: 'Create a new item',
      message: 'Please enter the following information to continue:',
      values: {}
    };

    for (const column of this.columns) {
      const prop = this.getPropName(column);
      data.values[prop] = {
        label: column.name
      };
    }

    this.dialog
      .open(InputDialogComponent, {
        data: data
      })
      .afterClosed()
      .subscribe(result => {
        if (result) {
          this.create.emit(result);
        }
      });
  }

  onDelete(row) {
    this.dialog
      .open(ConfirmDialogComponent, {
        data: {
          title: 'Confirmation',
          message: 'Are you sure you want to delete this configuration?'
        }
      })
      .afterClosed()
      .subscribe(result => {
        if (result) {
          this.delete.emit({
            row: row
          });
        }
      });
  }

  getPropName(column): string {
    return column.prop ? column.prop : column.name.toLowerCase();
  }

}
