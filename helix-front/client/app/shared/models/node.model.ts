import * as _ from 'lodash';

interface SimpleFieldObject {
  name: string;
  value: string;
}

interface ListFieldObject {
  name: string;
  value: SimpleFieldObject[];
}

interface MapFieldObject {
  name: string;
  value: SimpleFieldObject[];
}

// This is a typical Helix Node definition
export class Node {
  id: string;
  simpleFields: SimpleFieldObject[];
  listFields: ListFieldObject[];
  mapFields: MapFieldObject[];

  constructor(obj: any) {
    if (obj != null) {
      this.id = obj.id;
      this.simpleFields = this.keyValueToArray(obj.simpleFields);

      this.listFields = [];
      _.forOwn(obj['listFields'], (v, k) => {
        this.listFields.push(<ListFieldObject>{
          name: k,
          value: _.map(v, item => {
            return <SimpleFieldObject>{
              value: item
            };
          })
        });
      });

      this.mapFields = [];
      _.forOwn(obj['mapFields'], (v, k) => {
        this.mapFields.push(<MapFieldObject>{
          name: k,
          value: this.keyValueToArray(v)
        });
      });
    }
  }

  // Converting raw simpleFields to SimpleFieldObject[]
  private keyValueToArray(obj: Object): SimpleFieldObject[] {
    const result: SimpleFieldObject[] = [];
    for (const k in obj) {
      if (obj.hasOwnProperty(k)) {
        result.push(<SimpleFieldObject>{
          name: k,
          value: obj[k]
        });
      }
    }
    return result;
  }
}
