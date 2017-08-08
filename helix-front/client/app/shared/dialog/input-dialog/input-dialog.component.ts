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
  value: string;

  constructor(
    @Inject(MD_DIALOG_DATA) protected data: any,
    public dialogRef: MdDialogRef<InputDialogComponent>
  ) { }

  ngOnInit() {
    this.title = (this.data && this.data.title) || 'Input';
    this.message = (this.data && this.data.message) || 'Please enter:';
  }

}
