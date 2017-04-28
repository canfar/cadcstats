#
from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import scan
#
import requests
import pandas as pd
import numpy as np
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

def fig1(conn, idx, svc):
	service = {"ssos" : ["ssos", "ssosclf.pl"], "dss" : ["dss", "dss_status.py"], "meeting": ["getMeetings.html", "meetingsvc"], "yes" : ["YorkExtinctionSolver", "output.cgi"], "vosui" : ["vosui", ["storage", "vosui"]], "adv" : [["en", "search"], ["tap", "sync"]]}
	ttl = {"ssos" : "Solar System Object Image Search", "dss" : "Digital Sky Survey System", "meeting" : "Meetings", "yes" : "York Extinction Solver", "adv":"Advanced Search", "vosui" : "VOS Web Browser"}
	plts = [Div(text = "<h1>{}</h1>".format(ttl[svc]), width = 1000)]

	for req in service[svc]:
		df = pd.DataFrame()
		df2 = pd.DataFrame()
		
		for dom in ["internal", "external"]:

			query = {
				"size" : 0,
				"query" : {
					"bool" : {
						"filter" : [
							{ "match" : {"request" : "{}".format(req)} }
						],
						"must_not" : [
							{ "regexp" : { "agent" : ".*(bot|spider|Bot|BOT|Spider|SPIDER).*" }}
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
			if svc == "vosui":
				query["query"]["bool"]["filter"] = [{ "match" : {"referrer" : service[svc][0] } }]
				if req == service[svc][1]:
					query["query"]["bool"].setdefault("filter", []).append({ "regexp" : { "request.keyword" : "\/({}|{}).*".format(req[0], req[1])} })

			if svc == "adv":
				query["query"]["bool"]["filter"] = [ { "regexp" : { "referrer.keyword" : ".*\/en\/search\/"} } ]
				if req == service[svc][1]:
					query["query"]["bool"].setdefault("filter", []).append({ "regexp" : {"request.keyword" : "\/{}\/{}.*\/run".format(req[0], req[1]) } })

			if svc == "meeting" and req != service[svc][0] :
				query["query"]["bool"].setdefault("filter", []).append({ "match" : { "referrer" : "editMeetings.html" } })

			if dom == "internal":
				query["query"]["bool"]["minimum_should_match"] = 1
				query["query"]["bool"]["should"] = [
					{ "term" : { "geoip.ip" : "132.246.0.0/16"}},
					{ "term" : { "geoip.ip" : "206.12.0.0/16"}},
					{ "term" : { "geoip.ip" : "192.168.0.0/16"}}
				]
			else:
				query["query"]["bool"]["must_not"] = [
					{ "regexp" : { "agent" : ".*(bot|spider|Bot|BOT|Spider|SPIDER).*" }},
					{ "term" : { "geoip.ip" : "132.246.0.0/16"}},
					{ "term" : { "geoip.ip" : "206.12.0.0/16"}},
					{ "term" : { "geoip.ip" : "192.168.0.0/16"}}
				]

			try:
				res = conn.search(index = idx, body = query)
			except TransportError as e:
				print(e.info)
				raise

			for _ in res["aggregations"]["peryr"]["buckets"]:
			 	df = df.append(pd.DataFrame([[_["doc_count"]]], columns = [dom], index = [_["key_as_string"]]))
			 	df2 = df2.append(pd.DataFrame([[_["unq_ip"]["value"]]], columns = [dom], index = [_["key_as_string"]])) 	

		for i, _ in enumerate([df, df2]):
			_ = _.groupby(_.index).sum().fillna(0).reset_index()
			_ = _.rename(columns = {"index":"time"})

			p = Bar(data = _, 
					values = blend("external", "internal", name = "Page Hits", labels_name = "domain"), 
					label = cat(columns = "time", sort = False), 
					stack = cat(columns = "domain", sort = False), 
					color = color(columns = "domain", palette = clr, sort = False), 
					legend = "top_right", title = "{}".format("Page Hits" if req == service[svc][0] else "Service Use"), 
					tooltips = [("domain", "@domain"), ("time", "@time"), ("hits", "@height")],
					plot_width = 1200
					)
			p.yaxis.axis_label = "{}".format("Number of Events" if i == 0 else "Number of Unique IPs")
			plts.append(p)

	# output_file("fig1.html")
	# show(column([plts[0]] + plts[1::2] + plts[2::2]))
	return column([plts[0]] + plts[1::2] + plts[2::2])
	
if __name__ == "__main__":
	conn = Init().connect()
	fig1(conn, "logs-apache", "yes")

