curl -XPUT http://localhost:9200/logs-condor -d '
{
"template": "condor-logs",
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
		"CommittedSuspensionTime": {
			"type": "long",
			"doc_values": true
		},
        "CumulativeSlotTime": {
          "type": "long",
          "doc_values": true
        },
		"NumJobStarts": {
          "type": "long",
          "doc_values": true
        },
		"DiskUsage": {
          "type": "long",
          "doc_values": true
        },
		"CommittedTime": {
          "type": "long",
          "doc_values": true
        },
		"RequestCpus": {
          "type": "long",
          "doc_values": true
        },
		"QDate": {
          "type": "long",
          "doc_values": true
        },
		"RequestMemory": {
          "type": "long",
          "doc_values": true
        },
		"Owner": {
          "type": "text",
          "fielddata": true,
		  "analyzer": "lowercase_whitespace"
        },
		"JobStartDate": {
          "type": "long",
          "doc_values": true
        },
		"VMName": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"VMInstanceName": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"VMInstanceType": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"Project": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"JobStatus": {
	      "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"RemoveReason": {
		  "type": "string",
          "analyzer": "lowercase_whitespace"
		},
		"CompletionDate": {
          "type": "long",
          "doc_values": true
        },
		"VMSpec.CPU": {
          "type": "long",
          "doc_values": true
        },
		"VMSpec.RAM": {
          "type": "long",
          "doc_values": true
        },
		"VMSpec.DISK": {
          "type": "long",
          "doc_values": true
        },
		"VMSpec.SCRATCH": {
          "type": "long",
          "doc_values": true
        },
		"MemoryUsage": {
          "type": "long",
          "doc_values": true
        },
		"RequestDisk": {
          "type": "long",
          "doc_values": true
        },
		"JobDuration": {
          "type": "long",
          "doc_values": true
        },
        "@timestamp": {
          "type": "date",
          "format": "dateOptionalTime"
        }
      }
    }
  }
}
'
