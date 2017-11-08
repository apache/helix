import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'hi-disabled-label',
  templateUrl: './disabled-label.component.html',
  styleUrls: ['./disabled-label.component.scss']
})
export class DisabledLabelComponent implements OnInit {

  @Input()
  text: string;

  constructor() { }

  ngOnInit() {
  }

}
