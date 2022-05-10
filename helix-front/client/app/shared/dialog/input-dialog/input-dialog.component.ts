import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef } from '@angular/material';
import { MAT_DIALOG_DATA } from '@angular/material';

@Component({
  selector: 'hi-input-dialog',
  templateUrl: './input-dialog.component.html',
  styleUrls: ['./input-dialog.component.scss']
})
export class InputDialogComponent implements OnInit {

  title: string;
  message: string;
  values: any[];

  constructor(
    @Inject(MAT_DIALOG_DATA) protected data: any,
    protected dialogRef: MatDialogRef<InputDialogComponent>
  ) { }

  ngOnInit() {
    this.title = (this.data && this.data.title) || 'Input';
    this.message = (this.data && this.data.message) || 'Please enter:';
    this.values = (this.data && this.data.values) || {
      'input': {
        label: 'Anything you want',
        type: 'input'
      }
    };
  }

  onSubmit() {
    this.dialogRef.close(this.values);
  }

  onCancel() {
    this.dialogRef.close();
  }

  getKeys(obj: any) {
    return Object.keys(obj);
  }

}
