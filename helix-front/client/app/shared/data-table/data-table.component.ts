import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';

import { Settings } from '../../core/settings';

@Component({
  selector: 'hi-data-table',
  templateUrl: './data-table.component.html',
  styleUrls: ['./data-table.component.scss']
})
export class DataTableComponent implements OnInit {

  @Input() rows = [];
  @Input() columns = [];
  @Input() sorts = [];

  @Output() update: EventEmitter<any> = new EventEmitter<any>();

  rowHeight = Settings.tableRowHeight;

  constructor() { }

  ngOnInit() {
  }

  onEdited(row, column, value) {
    const prop = column.prop ? column.prop : column.name.toLowerCase();

    // only emit when value changes
    if (row[prop] !== value) {
      this.update.emit({
        row: row,
        column: column,
        value: value
      });
    }
  }

}
