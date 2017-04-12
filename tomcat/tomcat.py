#!/Users/will/anaconda3/bin/python

from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import scan
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


odin = 'http://odin.cadc.dao.nrc.ca:9200'

class Init():
	def __init__(self, url = None, timeout = 120):
		self.timeout = timeout
		if not url:
			self.url = odin
		else:
			self.url = url	
		if not requests.get(self.url):
			print("Connection incorrect!")
			exit(0)
	def connect(self):
		return Elasticsearch(self.url, timeout = self.timeout)

def ip2dom(ip):
	try: 
		if ipv4(ip) >= ipv4("132.246.0.0") and ipv4(ip) <= ipv4("132.246.255.255"):
			if (ipv4(ip) >= ipv4("132.246.195.0") and ipv4(ip) <= ipv4("132.246.195.24")) or (ipv4(ip) >= ipv4("132.246.217.0") and ipv4(ip) <= ipv4("132.246.217.24")) or (ipv4(ip) >= ipv4("132.246.194.0") and ipv4(ip) <= ipv4("132.246.194.24")):
				return "CADC"
			else:
				return "NRC"		
		elif ipv4(ip) >= ipv4("206.12.0.0") and ipv4(ip) <= ipv4("206.12.255.255"):
			return "CC"
		elif ipv4(ip) >= ipv4("192.168.0.0") and ipv4(ip) <= ipv4("192.168.255.255"):
			return "CADC"	
		else:
			return "Others"	
	except AddressValueError:
		print("ip address cannot be handled {0}".format(ip))
		return "Error"

def timing(func):
	def wrapper(*args):
		t_i = time.time()
		r = func(*args)
		t_f = time.time() - t_i
		print("{0} took {1:.3f}s".format(func.__name__, t_f))
		return r
	return wrapper	

def fig1(conn, idx):
	method = ["PUT","GET"]
	service = ["transfer_ws", "data_ws", "vospace_ws"]
	p = 1
	plots = []
	for j, s in enumerate(service):
		for i, m in enumerate(method):
			query = {
					"query" : {
				        "bool" : {
				        	"must" : [
				            	{ "term" : { "service" : s } },
				            	{ "term" : { "phase" : "END" } },
				            	{ "term" : { "method" : m } }
				            ]
				        }	
				    },
				    "aggs": {
				    	"req_by_dom": {
				    		"terms": {"field": "clientdomain", "size": 6}
				    	}

				    }
				}
			try:
				res = conn.search(index = idx, body = query)
			except 	TransportError as e:
				print(e.info)
				raise
			df = pd.DataFrame.from_dict(res["aggregations"]["req_by_dom"]["buckets"])
			_ = pd.DataFrame([res["aggregations"]["req_by_dom"]["sum_other_doc_count"], "Others"]).T
			_.columns = df.columns
			df = df.append(_, ignore_index = True)
			df.columns = ["Events", "Domains"]
			plots.append(Donut(df, label = "Domains", values = "Events", title = "service: {0}, method: {1}".format(s, m)))
	grid = gridplot(plots, ncols = 2, plot_width = 600, plot_height = 600, title = "IS THIS A TITLE? nooooo its not working asdaw34q2AEWTQ!#@$$@%")
	output_file("fig1.html")
	show(column(Div(text = "<h1>BIG ASS TITLE</h1>"), grid))

def fig2(conn, idx):
	service = ["transfer_ws", "data_ws", "vospace_ws"]
	method = ["GET", "PUT"]
	pos = [0, 1, -1]
	clr = ["blue", "purple", "green"]
	plots = []
	for j, s in enumerate(service):
		for i, m in enumerate(method):
			query = {
					"query" : {
				        "bool" : {
				        	"must" : [
				            	{ "term" : { "service" : s } },
				            	{ "term" : { "phase" : "END" } },
				            	{ "term" : { "method" : m } }
				            ]
				        }	
				    },
					"aggs" : {
						"avgdur_perwk" : {
							"date_histogram" : {
					    		"field" : "@timestamp",
							    "interval" : "1W",
							    "format" : "yyyy-MM-dd" 
							},
							"aggs": {
								"avgdur" : { 
									"avg" : { 
										"field" : "duration"
									}
								}
							}
						}	
					}
				}
			try:
				res = conn.search(index = idx, body = query)
			except 	TransportError as e:
				print(e.info)
				raise	

			wk = [_["key_as_string"] for _ in res["aggregations"]["avgdur_perwk"]["buckets"]]
			avg_dur = [_["avgdur"]["value"] for _ in res["aggregations"]["avgdur_perwk"]["buckets"]]
			df = pd.DataFrame(list(zip(wk, avg_dur)), columns = ["time", "avg_dur"])
			df["avg_dur"] = df["avg_dur"] / 1000
			plots.append(Bar(df, "time", "avg_dur", legend = False, xlabel = None, yscale = "log", ylabel = "Average Duration", title = "Average Duration per Week (Sec): service: {0}, method: {1}".format(service[j], method[i])))	
	grid = gridplot(plots, ncols = 1, plot_width = 1200, plot_height = 300)
	output_file("fig2.html")
	show(column(Div(text = "<h1>BIG ASS TITLE</h1>"), grid))			

def fig3(conn, idx):
	#fig = plt.figure(figsize = (10, 5))
	#ax = fig.add_subplot(111)
	query = {
			"query" : {
		        "bool" : {
		        	"must" : [
		            	{ "term" : { "service" : "transfer_ws" } },
		            	{ "term" : { "phase" : "END" } },
		            	{ "term" : { "method" : "GET"} },
		            	{ "term" : { "clientip" : "206.12.48.85" } }
		            ]
		        }	
		    },
			"aggs" : {
				"avgrate_perwk" : {
					"date_histogram" : {
		                "field" : "@timestamp",
		                "interval" : "week",
		                "format" : "yyyy-MM-dd" 
		            },
					"aggs": {
						"avgrate" : { 
							"avg" : { 
								"field" : "rate"
							}
						}
					}
				}	
			}
		}
	try:
		res = conn.search(index = idx, body = query)
	except TransportError as e:
		print(e.info)
		raise	
	#print(res["aggregations"]["avgrate_perwk"]["buckets"])
	wk = [_["key_as_string"] for _ in res["aggregations"]["avgrate_perwk"]["buckets"]]
	avg_rate = [_["avgrate"]["value"] for _ in res["aggregations"]["avgrate_perwk"]["buckets"]]
	df = pd.DataFrame(list(zip(wk, avg_rate)), columns = ["time", "avg_rate"]).set_index("time")

	query2 = {
		"query" : {
			"match_all": {}
		},
		"aggs" : {
			"numjobs_perwk" : {
				"date_histogram" : {
		    		"field" : "@timestamp",
				    "interval" : "week",
				    "format" : "yyyy-MM-dd"
				}
			}	
		}
	}
	conn2 = Elasticsearch("http://206.12.59.36:9200")
	try:
		res = conn2.search(index = "logs-condor", body = query2)
	except TransportError as e:
		print(e.info)
		raise
	#print(res)	
	wk2 = [_["key_as_string"] for _ in res["aggregations"]["numjobs_perwk"]["buckets"]]
	numjobs = [_["doc_count"] for _ in res["aggregations"]["numjobs_perwk"]["buckets"]]
	df2 = pd.DataFrame(list(zip(wk2, numjobs)), columns = ["time", "numjobs"]).set_index("time")

	df = df.join(df2)
	df = df[pd.notnull(df["numjobs"])].fillna(0)
	
	x = [_ for _ in range(len(df))]
	p = figure(plot_width = 1200, toolbar_location = "above")
	p.vbar(x = x, top = df["avg_rate"], bottom = 0, width = 0.5, legend = "Avg Rate")
	p.y_range = Range1d(0, df["avg_rate"].max() * 1.3)
	p.yaxis.axis_label = "Average Transfer Rate"
	p.extra_y_ranges = {"right_yaxis": Range1d(0, df["numjobs"].max() * 1.1)}
	p.add_layout(LinearAxis(y_range_name = "right_yaxis", axis_label = "Number of Batch Jobs"), "right")
	p.line(x = x, y = df["numjobs"], line_width = 2, y_range_name = "right_yaxis", color = "red", legend = "Batch Jobs")
	p.legend.location = "top_left"
	d = dict(zip(x, df.index))
	p.xaxis[0].ticker = FixedTicker(ticks = x)
	p.xaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) + """
    if (tick in dic) {
        return dic[tick]
    }
    else {
        return ''
    }""")
	p.xaxis.major_label_orientation = np.pi/4
	output_file("fig3.html")
	show(column(Div(text = "<h1>Average Transfer Rate of <i>batch.canfar.net</i> VS Number of Batch Jobs</h1>", width = 1000), p))	

@timing
def fig4(conn, idx):
	
	iprange = {("132.246.194.0", "132.246.194.24"):"CADC", ("132.246.195.0", "132.246.195.24"):"CADC", ("132.246.217.0", "132.246.217.24"):"CADC", ("132.246.0.0", "132.246.255.255"):"NRC+CADC", ("192.168.0.0", "192.168.255.255"):"CADC-Private", ("206.12.0.0", "206.12.255.255"):"CC"}
	method = ["GET", "PUT"]
	i = 0
	plots = []
	for m in method:
		events, gbs = [], []
		for _ in iprange:
			query = {
					"query" : {
				        "bool" : {
				        	"must" : [
				            	{ "term" : { "service" : "transfer_ws" } },
				            	{ "term" : { "phase" : "END" } },
				            	{ "term" : { "method" : m } },
				            	{ "term" : { "success" : True } }
				            ]
					    }    
				    },
				    "aggs" : {
				    	"start_date" : {
							"min" : { "field" : "@timestamp" }
						},
						"end_date" : {
							"max" : { "field" : "@timestamp" }
						},
				        "ip_ranges" : {
				            "ip_range" : {
				                "field" : "clientip",
				                "ranges" : [
				                	{ "from" : _[0], "to" : _[1] }
				                ]
				            },
				            "aggs" : {
					            "gigabytes" : {
					        		"sum" : { "field" : "gbytes" }
					        	}
					        }	
				        },
				        "tot_giga" : {
				        	"sum" : { "field" : "gbytes" }
				        }
		    		}
				}

			try:
				res = conn.search(index = idx, body = query)
				#res = scan(conn, index = idx, query = query, scroll = "30m", size = 500)
			except TransportError as e:
				print(e.info)
				raise

			gbs.append({iprange[_]:res["aggregations"]["ip_ranges"]["buckets"][0]["gigabytes"]["value"]})
			events.append({iprange[_]:res["aggregations"]["ip_ranges"]["buckets"][0]["doc_count"]})
			
		tot_gbs = res["aggregations"]["tot_giga"]["value"]
		tot_events = res["hits"]["total"]
		start = res["aggregations"]["start_date"]['value_as_string']
		end = res["aggregations"]["end_date"]['value_as_string']

		df_gbs = pd.DataFrame.from_dict(gbs).sum().to_frame().T
		df_events = pd.DataFrame.from_dict(events).sum().to_frame().T
		df = pd.concat([df_gbs, df_events], ignore_index = True)

		df["NRC"] = df["NRC+CADC"] - df["CADC"]
		df["CADC"] = df["CADC"] + df["CADC-Private"]
		df["Others"] = pd.DataFrame([tot_gbs, tot_events])[0] - df["CADC"] - df["NRC"] - df["CC"]
		df = df[["CADC","NRC","CC", "Others"]].T.reset_index()
		df.columns = ["Domains", "Size", "Events"]

		for j in ["Size", "Events"]:
			p = Donut(df, label = "Domains", values = j, title = "Total: {0:.0f} {1:s}".format( (lambda: tot_gbs / 1024 if j == "Size" else tot_events / 1e6)(), (lambda: "TB" if j == "Size" else "million files")() ) )
			if i >= 2:
				p.add_layout( Title(text = "In {0:s}".format((lambda: "Size" if j == "Size" else "Number of Files")()), align = "center" ), "below" )
			if j == "Size":
				p.add_layout( Title(text = (lambda: "Downloads" if i == 0 else "Uploads")(), align = "center"), "left" )
			i += 1
			plots.append(p)
	grid = gridplot(plots, ncols = 2, plot_width = 600, plot_height = 600)
	output_file("fig4.html")
	show(column(Div(text = "<h1><center>Data Transfer From {0:s} To {1:s}</center></h1>".format(re.match("(\d{4}-\d{2}-\d{2})T", start).group(1), re.match("(\d{4}-\d{2}-\d{2})T", end).group(1)), width = 600), grid))			

def fig5(conn, idx):
	iprange = {("132.246.194.0", "132.246.194.24"):"CADC", ("132.246.195.0", "132.246.195.24"):"CADC", ("132.246.217.0", "132.246.217.24"):"CADC", ("132.246.0.0", "132.246.255.255"):"NRC+CADC", ("192.168.0.0", "192.168.255.255"):"CADC-Private", ("206.12.0.0", "206.12.255.255"):"CC"}
	service = ["data_ws", "vospace_ws"]
	method = ["GET", "PUT"]
	i = 0
	plots = []
	for m in method:
		for j, s in enumerate(service):
			events = []
			for _ in iprange:
				query = {
						"query" : {
					        "bool" : {
					        	"must" : [
					            	{ "term" : { "service" : s } },
					            	{ "term" : { "phase" : "END" } },
					            	{ "term" : { "method" : m } },
					            	{ "term" : { "success" : True } }
					            ]
						    }    
					    },
					    "aggs" : {
						    "start_date" : {
								"min" : { "field" : "@timestamp" }
							},
							"end_date" : {
								"max" : { "field" : "@timestamp" }
							},
					        "ip_ranges" : {
					            "ip_range" : {
					                "field" : "clientip",
					                "ranges" : [
					                	{ "from" : _[0], "to" : _[1] }
					                ]
					            }	
					        }
			    		}
					}

				try:
					res = conn.search(index = idx, body = query)
					#res = scan(conn, index = idx, query = query, scroll = "30m", size = 500)
				except TransportError as e:
					print(e.info)
					raise

				events.append({iprange[_]:res["aggregations"]["ip_ranges"]["buckets"][0]["doc_count"]})
				
			tot_events = res["hits"]["total"]
			start = res["aggregations"]["start_date"]['value_as_string']
			end = res["aggregations"]["end_date"]['value_as_string']

			df_events = pd.DataFrame.from_dict(events).sum().to_frame().T
			df = pd.concat([df_events], ignore_index = True)

			df["NRC"] = df["NRC+CADC"] - df["CADC"]
			df["CADC"] = df["CADC"] + df["CADC-Private"]
			df["Others"] = pd.DataFrame([tot_events])[0] - df["CADC"] - df["NRC"] - df["CC"]
			df = df[["CADC","NRC","CC", "Others"]].T.reset_index()
			df.columns = ["Domains", "Events"]

			p = Donut(df, label = "Domains", values = "Events", title = "Total Events: {}".format(tot_events) )
			if i >= 2:
				p.add_layout( Title(text = (lambda: "data_ws" if j == 0 else "vospace_ws")(), align = "center" ), "below" )
			if j == 0:
				p.add_layout( Title(text = (lambda: "Downloads" if i == 0 else "Uploads")(), align = "center"), "left" )
			i += 1
			plots.append(p)
	grid = gridplot(plots, ncols = 2, plot_width = 600, plot_height = 600)
	output_file("fig5.html")
	show(column(Div(text = "<h1>From {0:s} To {1:s}</h1>".format(re.match("(\d{4}-\d{2}-\d{2})T", start).group(1), re.match("(\d{4}-\d{2}-\d{2})T", end).group(1)), width = 600), grid))	

if __name__ == "__main__":
	conn = Init(timeout = 300).connect()
	#fig1(conn, "delivery_history-*")
	#fig2(conn, "delivery_history-*")
	#fig3(conn, "delivery_history-*")
	fig4(conn, "delivery_history-*")
	#fig5(conn, "delivery_history-*")