from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import scan
from elasticsearch_xpack import XPackClient
import requests
import pandas as pd
import numpy as np
import re
from ipaddress import IPv4Address as ipv4, AddressValueError
import time
from bokeh.plotting import figure, output_file, show, save
from bokeh.models import FuncTickFormatter, FixedTicker, NumeralTickFormatter, Div, Title, LinearAxis, Range1d
from bokeh.charts import Bar, Donut
from bokeh.layouts import gridplot, column, row

fields = ["observation_id","pi_name","proposal_id","proposal_title","proposal_keyword","data_release_date_public","data_release_date","observation_intention","target","target_upload","pixel_scale_left","pixel_scale_right","observation_date_left","observation_date_right","integration_time","time_span","spactral_coverage_left","spactral_coverage_right","spactral_sampling_left","spactral_sampling_right","resolving_power_left","resolving_power_right","bandpass_width_left","bandpass_width_right","rest_frame_energy_left","rest_frame_energy_right","band","collection","instrument","filter","calibration_level","data_type","observation_type"]

es = 'http://elastic:cadcstats@206.12.59.36:9200'
odin = 'http://odin.cadc.dao.nrc.ca:9200'

class Init():
	def __init__(self, url = None, timeout = 120):
		self.timeout = timeout
		if not url:
			self.url = es
		else:
			self.url = url	
		if not requests.get(self.url):
			print("Connection incorrect!")
			exit(0)
	def connect(self):
		return Elasticsearch(self.url, timeout = self.timeout)

# usage of each field of advancedsearch
def fig1(idx, conn):
	tot = conn.search(idx, body = { "query" : { "match_all" : {}}})["hits"]["total"]
	#print(tot)
	df = pd.DataFrame([[tot]], index = ["tot"])
	for f in fields:
		if f == "data_release_date_public" or f =="target_upload":
			query = {
				"query" : {
					"bool" : {
						"must" : [
							{ "term" : {f : "true"}}
						]
					}
				}
			}
			df = df.append(pd.DataFrame([[ conn.search(idx, body = query)["hits"]["total"] ]], index = [f]))
		elif f == "observation_intention":
			query = {
				"query" : {
					"match_all": {}
				},
				"aggs": {
					"obs_int": {
						"terms": {
							"field": "observation_intention.keyword",
							"size": 10
						}
					}
				}
			}
			for _ in conn.search(idx, body = query)["aggregations"]["obs_int"]["buckets"]:
				if _["key"] != "both":
					df = df.append(pd.DataFrame([[ _["doc_count"] ]], index = [f + "_" + _["key"]]))
		else:	
			query = {
				"query" : {
					"exists" : {
						"field" : f
					}
				}
			}
			df = df.append(pd.DataFrame([[ conn.search(idx, body = query)["hits"]["total"] ]], index = [f]))
	print(df)

def test(idx, conn):
	query = {
	    "query" : {
	        "bool" : {
	            "must": [ 
	                { "term" : { "service.keyword" : "transfer_ws" } },
	                { "term" : { "method.keyword" : "GET" } },
	                { "range": { "from.keyword": { "gte": "206.12.0.0", "lte": "206.12.255.255"} } }
	            ]
	            # ,
	            # "must_not" : [
	            #     #{ "range": { "from.keyword": { "gte": "132.246.194.0", "lte": "132.246.194.24"} } },
	            #     { "range": { "from.keyword": { "gte": "206.12.0.0", "lte": "206.12.255.255"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.0.0", "lte": "132.246.255.255"} } }
	            # ]
	            # ,
	            # "should" : [
	            #     { "range": { "from.keyword": { "gte": "132.246.194.0", "lte": "132.246.194.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.195.0", "lte": "132.246.195.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.217.0", "lte": "132.246.217.24"} } }
	            # ]
	            # ,
	            # "must_not" : [
	            #     { "range": { "from.keyword": { "gte": "132.246.194.0", "lte": "132.246.194.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.195.0", "lte": "132.246.195.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.217.0", "lte": "132.246.217.24"} } }
	            # ]
	            ,
	            "must_not" : [
	            	{ "match_phrase" : { "path" : "/mnt/tmp/new" } }
	            ]
	        }
	    }
	    ,
	    "aggs" : {
	        "unq_dl" : {
	            "terms" : {
	                "field" : "path.keyword",
	                "size": 40000
	            }
	            ,
	            "aggs" : {
	            	"unq_sum" : {
	            		"sum" : {
	            			"field" : "bytes"
	            		}
	            	}
	            }
	        }
	    }
	}
    
	res = conn.search(index = idx, body = query) 
	r = 0
	for _ in res["aggregations"]["unq_dl"]["buckets"]:
		r += _["unq_sum"]["value"] / _["doc_count"]
	print(r / 1024 / 1024 / 1024 / 1024)

if __name__ == "__main__":
	conn = Init(timeout = 3000).connect()
	#fig1("logs-advancedsearch", conn)
	test("logs-tomcat", conn)		