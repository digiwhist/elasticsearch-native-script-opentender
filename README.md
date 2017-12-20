# Elasticsearch 2.4.6 Plugin for Opentender

based on https://github.com/imotov/elasticsearch-native-script-example

## Installation

run command `plugin install file:///path/to/elasticsearch-native-script-opentender-2.4.6.zip`

Note: "plugin is the elasticsearch plugin helper binary"

## Deinstallation

run command `plugin remove elasticsearch-native-script-opentender`

## Build

The plugin can be built using mvn package command. The assembled .zip package can be found in the target/releases/ directory

## About

This is a ElasticSearch Native implementation of this groovy script aggregation. It's way faster.

```javascript

{
	"params": {
		"_fields": fields,
		"_weights": weights,
		"_agg": {}
	},
	"init_script": `_agg['sum'] = 0; _agg['count'] = 0`,
	"map_script":
		`   sum = 0;
			sum_weights = 0;
			for (i = 0; i < _fields.size(); i++) {
				weight = _weights[i];
				indicator = doc[fields[i]];
				if (!indicator.empty) {
					sum += indicator.value * weight;
					sum_weights += weight;
				}
			}
			if (sum_weights > 0) {
				_agg['sum'] += sum/sum_weights;
				_agg['count'] += 1;
			}`,
	"reduce_script":
		`	sum = 0;
			count = 0;
			for (a in _aggs) {
				sum += a.sum;
				count += a.count;
			}
			return count>0?(sum/count):null;`
}
```
