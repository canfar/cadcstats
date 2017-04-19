#
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import scan
from elasticsearch_xpack import XPackClient
#
import requests
import pandas as pd
import numpy as np
import re
import json
import time
from ipaddress import IPv4Address as ipv4, AddressValueError
#
from bokeh.plotting import figure, output_file, show, save
from bokeh.models import FuncTickFormatter, FixedTicker, NumeralTickFormatter, Div, Title, LinearAxis, Range1d
from bokeh.charts import Bar, Donut
from bokeh.layouts import gridplot, column, row
from bokeh.charts.attributes import cat, color
from bokeh.charts.operations import blend

es = 'http://elastic:cadcstats@206.12.59.36:9200'
clr = ["blue", "purple", "orange", "green"]

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

def fig1(idx, conn, svc):
	service = {"ssos" : ["ssos", "ssosclf.pl"], "dss" : ["dss", "dss_status.py"], "community": ["community"]}
	ttl = {"ssos" : "Solar System Object Image Search", "dss" : "Digital Sky Survey System", "community" : "Community"}
	plts = [Div(text = "<h1>{}</h1>".format(ttl[svc]), width = 1000)]
	for req in service[svc]:
		df = pd.DataFrame()
		df2 = pd.DataFrame()
		for dom in ["CC", "CADC", "NRC", "Others"]:
			query = {
				"size" : 0,
				"query" : {
					"bool" : {
						"must" : [
							{ "match" : {"request" : "{}".format(req)} }
						]
					}		
				},
				"aggs" : {
					"peryr" : {
						"date_histogram" : {
				    		"field" : "@timestamp",
						    "interval" : "month",
						    "format" : "yyyy-MM" 
						},
						"aggs" : {
							"unq_ip" : {
								"cardinality" : {
									"field" : "geoip.ip"
								}
							}
						}
					}
				}
			}
			if svc == "community":
				query["query"]["bool"].setdefault("must", []).append({ "range" : { "@timestamp" : { "gte" : "2014-12"} } })
			if dom == "CADC":
				query["query"]["bool"]["minimum_should_match"] = 1
				query["query"]["bool"]["should"] = [
						{ "term" : { "geoip.ip" : "132.246.194.0/24"}},
						{ "term" : { "geoip.ip" : "132.246.195.0/24"}},
						{ "term" : { "geoip.ip" : "132.246.217.0/24"}},
						{ "term" : { "geoip.ip" : "192.168.0.0/16"}}
					]
			elif dom == "CC":
				query["query"]["bool"].setdefault("must", []).append({ "term" : { "geoip.ip" : "206.12.0.0/16" } })
			elif dom == "NRC":
				query["query"]["bool"].setdefault("must", []).append({ "term" : { "geoip.ip" : "132.246.0.0/16" } })
				query["query"]["bool"]["must_not"] = [
					{ "term" : { "geoip.ip" : "132.246.194.0/24"}},
					{ "term" : { "geoip.ip" : "132.246.195.0/24"}},
					{ "term" : { "geoip.ip" : "132.246.217.0/24"}}
				]
			else:
				query["query"]["bool"]["must_not"] = [
					{ "regexp" : { "agent" : ".*(bot|spider|Bot|BOT|Spider|SPIDER).*" }},
					{ "term" : { "geoip.ip" : "132.246.0.0/16"}},
					{ "term" : { "geoip.ip" : "206.12.0.0/16"}},
					{ "term" : { "geoip.ip" : "192.168.0.0/16"}}
				]
				#print(json.dumps(query, indent = 4))
			res = conn.search(index = idx, body = query)

			for _ in res["aggregations"]["peryr"]["buckets"]:
			 	df = df.append(pd.DataFrame([[_["doc_count"]]], columns = [dom], index = [_["key_as_string"]]))
			 	df2 = df2.append(pd.DataFrame([[_["unq_ip"]["value"]]], columns = [dom], index = [_["key_as_string"]])) 	

		df = df.groupby(df.index).sum().fillna(0).apply(np.log10).replace([np.inf, -np.inf], np.nan).reset_index()
		df = df.rename(columns = {"index":"time"})

		p = Bar(data = df, 
				values = blend("CADC", "NRC", "CC", "Others", name = "Page Hits", labels_name = "domain"), 
				label = cat(columns = "time", sort = False), 
				stack = cat(columns = "domain", sort = False), 
				color = color(columns = "domain", palette = clr, sort = False), 
				legend = "top_right", title = "{}".format("Page Hits" if req == service[svc][0] else "Service Use"), 
				tooltips = [("domain", "@domain"), ("time", "@time"), ("hits", "@height")],
				plot_width = 1200
				)
		p.yaxis.axis_label = "Log10 of {}".format("Page Hits" if req == service[svc][0] else "Service Use")
		plts.append(p)

		df2 = df2.groupby(df2.index).sum().fillna(0).reset_index()
		df2 = df2.rename(columns = {"index":"time"})
		#print(df2)

		p = Bar(data = df2, 
				values = blend("CADC", "NRC", "CC", "Others", name = "Unique IPs", labels_name = "domain"), 
				label = cat(columns = "time", sort = False), 
				stack = cat(columns = "domain", sort = False), 
				color = color(columns = "domain", palette = clr, sort = False), 
				legend = "top_right", title = "{} Unique IPs".format("Page Hits" if req == service[svc][0] else "Service Use"), 
				tooltips = [("domain", "@domain"), ("time", "@time"), ("hits", "@height")],
				plot_width = 1200
				)
		p.yaxis.axis_label = "Number of {} Unique IPs".format("Page Hits" if req == service[svc][0] else "Service Use")
		plts.append(p)

	output_file("fig1.html")
	show(column([plts[0]] + plts[1::2] + plts[2::2]))
	
if __name__ == "__main__":
	conn = Init().connect()
	fig1("logs-apache", conn, "community")

