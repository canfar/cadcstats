curl -XPUT http://localhost:9200/logs-advancedsearch -d '
{
"template": "advancedsearch-logs",
  "settings": {
    "analysis": {
      "analyzer": {
        "lowercase_whitespace": {
          "type": "custom",
          "tokenizer": "whitespace",
          "filter": [
            "lowercase"
          ]
        }
      }
    },
    "index": {
      "number_of_replicas": 1
    }
  },
  "mappings": {
    "_default_": {
      "properties": {
		"jobid": {
          "type": "text",
          "fielddata": true,
		  "analyzer": "lowercase_whitespace"
        },
		"runid": {
          "type": "text",
          "fielddata": true,
		  "analyzer": "lowercase_whitespace"
        },
		"ownerid": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"phase": {
          "type": "text",
          "fielddata": true,
		  "analyzer": "lowercase_whitespace"
        },
		"duration": {
          "type": "integer",
		  "doc_values": true
        },
		"observation_id": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"pi_name": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"proposal_id": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"proposal_title": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"proposal_keyword": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"observation_intention": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"target": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"remoteip": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"target_upload": {
		  "type": "boolean",
		  "doc_values": true
		},
		"data_release_date_public": {
          "type": "boolean",
          "doc_values": true
        },
		"pixel_scale_left": {
          "type": "double",
          "doc_values": true
        },
		"pixel_scale_right": {
          "type": "double",
          "doc_values": true
        },
		"observation_date_left": {
          "type": "double",
          "doc_values": true
        },
        "observation_date_right": {
          "type": "double",
          "doc_values": true
        },
		"integration_time": {
          "type": "double",
          "doc_values": true
        },
		"time_span": {
          "type": "double",
          "doc_values": true
        },
		"spactral_coverage_left": {
          "type": "double",
          "doc_values": true
        },
		"spactral_coverage_right": {
          "type": "double",
          "doc_values": true
        },
		"spactral_sampling_left": {
          "type": "double",
          "doc_values": true
        },
        "spactral_sampling_right": {
          "type": "double",
          "doc_values": true
        },
		"resolving_power_left": {
          "type": "double",
          "doc_values": true
        },
        "resolving_power_right": {
          "type": "double",
          "doc_values": true
        },
		"bandpass_width_left": {
          "type": "double",
          "doc_values": true
        },
        "bandpass_width_right": {
          "type": "double",
          "doc_values": true
        },
		"rest_frame_energy_left": {
          "type": "double",
          "doc_values": true
        },
        "rest_frame_energy_right": {
          "type": "double",
          "doc_values": true
        },
		"band": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"collection": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"instrument": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"filter": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"data_type": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"observation_type": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"calibration_level": {
          "type": "integer",
          "doc_values": true
        },
        "@timestamp": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "data_release_date": {
		  "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        }
      }
    }
  }
}
'
