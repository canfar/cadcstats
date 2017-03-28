curl -XPUT http://192.168.59.224:9200/logs-tomcat -d '
{
"template": "tomcat-logs",
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
		"service": {
          "type": "text",
          "fielddata": true,
		  "analyzer": "lowercase_whitespace"
        },
		"servlet": {
          "type": "text",
          "fielddata": true,
		  "analyzer": "lowercase_whitespace"
        },
		"user": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"success": {
		  "type": "boolean",
		  "doc_values": true
		},
		"bytes": {
          "type": "double",
          "doc_values": true
        }
		"method": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"path": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"from": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"message": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
        "@timestamp": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "jobID": {
		  "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        },
		"target": {
          "type": "text",
          "fielddata": true,
          "analyzer": "lowercase_whitespace"
        }
      }
    }
  }
}
'
