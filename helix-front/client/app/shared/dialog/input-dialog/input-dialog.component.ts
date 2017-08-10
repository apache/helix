import { Component, OnInit, Inject } from '@angular/core';
import { MdDialogRef } from '@angular/material';
import { MD_DIALOG_DATA } from '@angular/material';

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
    @Inject(MD_DIALOG_DATA) protected data: any,
    protected dialogRef: MdDialogRef<InputDialogComponent>
  ) { }

  ngOnInit() {
    this.title = (this.data && this.data.title) || 'Input';
    this.message = (this.data && this.data.message) || 'Please enter:';
    this.values = (this.data && this.data.values) || {
      'input': {
        label: 'Anything you want'
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
