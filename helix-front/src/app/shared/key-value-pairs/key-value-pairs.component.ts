import { Component, ContentChildren, Directive, Input, QueryList } from '@angular/core';

import * as _ from 'lodash';

// good doc: https://angular.io/docs/ts/latest/api/core/index/ContentChildren-decorator.html

@Directive({ selector: 'hi-key-value-pair' })
export class KeyValuePairDirective{
  @Input() name: string;
  @Input() prop: string;
}

@Component({
  selector: 'hi-key-value-pairs',
  templateUrl: './key-value-pairs.component.html',
  styleUrls: ['./key-value-pairs.component.scss'],
})
export class KeyValuePairsComponent {

  getProp = _.get;

  @ContentChildren(KeyValuePairDirective) pairs: QueryList<KeyValuePairDirective>;

  @Input() obj: any;
}
