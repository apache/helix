import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'hi-state-label',
  templateUrl: './state-label.component.html',
  styleUrls: ['./state-label.component.scss']
})
export class StateLabelComponent implements OnInit {

  @Input() state: string;
  @Input() isReady: boolean;

  constructor() { }

  ngOnInit() {
  }

}
