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

def test(idx, conn):
	query = {
		"size" : 0,
	    "query" : {
	        "bool" : {
	            "must": [ 
	                { "term" : { "service.keyword" : "transfer_ws" } },
	                { "term" : { "method.keyword" : "GET" } }
	       			,
	       			{
		      		"query_string" : {
				        	"query" : "NOT from:(132.246* OR 192.168* OR 206.12*)"
				        }
				    }
	    			# ,
					# { "term" : { "phase" : "END" } }
	                # { "range" : { "@timestamp" : { "gte" : "2015-12-27"} } },
	                # { "match_phrase_prefix" : { "from" : "132.246"}}
	                # { "range" : { "from.keyword" : { "gte" : "132.246.217.0", "lte" : "132.246.217.24"} } }
	            ]
	            # ,
	            # "must_not" : [
	            #     { "match_phrase_prefix": { "from": "132.246"} } ,
	            #     { "match_phrase_prefix": { "from": "192.168"} } ,
	            #     { "match_phrase_prefix": { "from": "206.12"} } 
	            # ]
	            # ,
	            # "should" : [
	            #     { "range": { "from.keyword": { "gte": "132.246.194.0", "lte": "132.246.194.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.195.0", "lte": "132.246.194.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.217.0", "lte": "132.246.217.24"} } }
	            # ]
	            # ,
	            # "must_not" : [
	            #     { "range": { "from.keyword": { "gte": "132.246.194.0", "lte": "132.246.194.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.195.0", "lte": "132.246.195.24"} } },
	            #     { "range": { "from.keyword": { "gte": "132.246.217.0", "lte": "132.246.217.24"} } }
	            # ]
	            # ,
	            # "must_not" : [
	            # 	{ "match_phrase" : { "path" : "/mnt/tmp/new" } }
	            # ]
	        }
	        
	    }
	    ,
	    "aggs" : {
	    	"tot_dl" : {
	    		"sum" : {
	    			"field" : "bytes"
	    		}
	    	}
	    }
	    # ,
	    # "aggs" : {
	    #     "unq_dl" : {
	    #         "terms" : {
	    #             "field" : "path.keyword",
	    #             "size": 24000000,
	    #             "execution_hint" : "global_ordinals_low_cardinality"
	    #         }
	    #         ,
	    #         "aggs" : {
	    #         	"unq_sum" : {
	    #         		"avg" : {
	    #         			"field" : "bytes"
	    #         		}
	    #         	}
	    #         }
	    #     }
	    #     ,
	    #     "sum_unique_size": {
	    #         "sum_bucket": {
	    #             "buckets_path": "unq_dl>unq_sum" 
	    #         }
	    #     }
	    # }
	    # ,
	    # "aggs" : {
	    # 	"unq_dl": {
	    # 		"cardinality" : {
	    # 			"field" : "path.keyword",
	    # 			"precision_threshold": 400000
	    # 		}
	    # 	}
	    # }
	}
    
	res = conn.search(index = idx, body = query) 
	r = 0
	#print(res["aggregations"]["sum_unique_size"]["value"] / 1024**4 , len(res["aggregations"]["unq_dl"]["buckets"]))
	print(res["hits"]["total"] / 1e6, res["aggregations"]["tot_dl"]["value"] / 1024**4)
	# for _ in res["aggregations"]["unq_dl"]["buckets"]:
	# 	r += _["unq_sum"]["value"] / _["doc_count"]
	# print(r / 1024 / 1024 / 1024 / 1024)
	#print(res)

if __name__ == "__main__":
	conn = Init(timeout = 1000).connect()
	#conn2 = Init(url = odin, timeout = 600).connect()
	#test("tomcat-svc-*", conn2)
	test("logs-tomcat", conn)		