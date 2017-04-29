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
my_es = 'http://users:cadcusers@206.12.59.36:9200'

class Init():
	def __init__(self, url = my_es, timeout = 120):
		self.timeout = timeout
		if url:
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

##
# fig1 would not work on my ES index, since i did not do reverse DNS at the time of ingestion
# nor do i have the "clientdomain" field
#

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
				        	"filter" : [
				            	{ "term" : { "service" : s } },
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
	show(column(Div(text = "<h1>Number of Data Transfers by Domain</h1>", width = 1200), grid))

def fig2(conn, idx):
	service = ["transfer_ws", "data_ws", "vospace_ws"]
	method = ["get", "put"]
	pos = [0, 1, -1]
	clr = ["blue", "purple", "green"]
	plots = []
	for j, s in enumerate(service):
		for i, m in enumerate(method):
			query = {
				"size" : 0,	
				"query" : {
			        "bool" : {
			        	"filter" : [
			            	{ "term" : { "service" : s } },
			            	{ "term" : { "method" : m } }
			            ]
			        }	
			    },
				"aggs" : {
					"avgdur_perwk" : {
						"date_histogram" : {
				    		"field" : "@timestamp",
						    "interval" : "month",
						    "format" : "yyyy-MM-dd" 
						},
						"aggs": {
							"avgdur" : { 
								"avg" : { 
									"field" : "time"
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

	return column(Div(text = "<h1>Time Evolution of Data Transfers</h1>", width = 1200), grid)		

def fig3(conn, idx):
	query = {
		"size" : 0,
		"query" : {
	        "bool" : {
	        	"filter" : [
	            	{ "term" : { "service" : "transfer_ws" } },
	            	{ "term" : { "method" : "get"} },
	            	{ "term" : { "from" : "206.12.48.85" } },
	            	{ "range" : { "time" : { "gt" : 0 } } },
	            	{ "range" : { "bytes" : { "gt" : 0 } } },
	            	{ "term" : { "success" : True } }
	            ]
	        }	
	    },
		"aggs" : {
			"avgrate_perwk" : {
				"date_histogram" : {
	                "field" : "@timestamp",
	                "interval" : "month",
	                "format" : "yyyy-MM-dd" 
	            },
				"aggs": {
					"avgrate" : { 
						"avg" : { 
							"script" : {
								"lang" : "painless",
								"inline" : "doc['bytes'].value / doc['time'].value" 
							}
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
	
	wk = [_["key_as_string"] for _ in res["aggregations"]["avgrate_perwk"]["buckets"]]
	avg_rate = [_["avgrate"]["value"] for _ in res["aggregations"]["avgrate_perwk"]["buckets"]]
	df = pd.DataFrame(list(zip(wk, avg_rate)), columns = ["time", "avg_rate"]).set_index("time")

	query2 = {
		"aggs" : {
			"numjobs_perwk" : {
				"date_histogram" : {
		    		"field" : "@timestamp",
				    "interval" : "month",
				    "format" : "yyyy-MM-dd"
				}
			}	
		}
	}
	try:
		res = conn.search(index = "logs-condor", body = query2)
	except TransportError as e:
		print(e.info)
		raise
	
	wk2 = [_["key_as_string"] for _ in res["aggregations"]["numjobs_perwk"]["buckets"]]
	numjobs = [_["doc_count"] for _ in res["aggregations"]["numjobs_perwk"]["buckets"]]
	df2 = pd.DataFrame(list(zip(wk2, numjobs)), columns = ["time", "numjobs"]).set_index("time")

	df = df.join(df2)
	df = df[pd.notnull(df["numjobs"])].fillna(0)
	df["avg_rate"] = df["avg_rate"] / 1000

	x = [_ for _ in range(len(df))]
	p = figure(plot_width = 1200, toolbar_location = "above")
	p.vbar(x = x, top = df["avg_rate"], bottom = 0, width = 0.5, legend = "Avg Rate")
	p.y_range = Range1d(0, df["avg_rate"].max() * 1.3)
	p.yaxis.axis_label = "Average Transfer Rate (MB/s)"
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

	return column(Div(text = "<h1>Average Transfer Rate of <i>batch.canfar.net</i> VS Number of Batch Jobs</h1>", width = 1000), p)	

#@timing
def fig4(conn, idx):	
	iprange = {"132.246.194*":"CADC", "132.246.195*":"CADC", "132.246.217*":"CADC", "132.246*":"NRC+CADC", "192.168*":"CADC-Private", "206.12*":"CC"}
	method = ["get", "put"]
	i = 0
	plots = []
	for m in method:
		events, gbs = [], []
		for _ in iprange:
			query = {
				"size" : 0,
				"query" : {
			        "bool" : {
			        	"filter" : [
			            	{ "term" : { "service" : "transfer_ws" } },
			            	{ "term" : { "method" : m } },
			            	{ "term" : { "success" : True } },
			            	{
					      		"query_string" : {
						        	"query" : "from:{}".format(_)
						        }
						    }
			            ]
				    }    
			    },
			    "aggs" : {
			        "bytes" : {
		        		"sum" : { "field" : "bytes" }
		        	}
	    		}
			}

			try:
				res = conn.search(index = idx, body = query)
			except TransportError as e:
				print(e.info)
				raise

			gbs.append({iprange[_]:res["aggregations"]["bytes"]["value"] / (1024 ** 4)})
			events.append({iprange[_]:res["hits"]["total"] / 1e6})
			
		query_tot_events = {
			"size" : 0,
			"query" : {
		        "bool" : {
		        	"filter" : [
		            	{ "term" : { "service" : "transfer_ws" } },
		            	{ "term" : { "method" : m } },
		            	{ "term" : { "success" : True } }
		            ]
			    }   
		    }
		}

		res = conn.search(index = idx, body = query_tot_events)
		tot_events = res["hits"]["total"] / 1e6	
		

		query_tot_gbs = {
			"size" : 0,
			"query" : {
		        "bool" : {
		        	"filter" : [
		            	{ "term" : { "service" : "transfer_ws" } },
		            	{ "term" : { "method" : m } },
		            	{ "term" : { "success" : True } }
		            ]
			    }    
		    },
		    "aggs" : {
		    	"tot_gbs" : {
		    		"sum" : {
		    			"field" : "bytes"
		    		}
		    	}
		    }
		}
		tot_gbs = conn.search(index = idx, body = query_tot_gbs)["aggregations"]["tot_gbs"]["value"] / (1024 ** 4)

		df_gbs = pd.DataFrame.from_dict(gbs).sum().to_frame().T
		df_events = pd.DataFrame.from_dict(events).sum().to_frame().T
		df = pd.concat([df_gbs, df_events], ignore_index = True)

		df["NRC"] = df["NRC+CADC"] - df["CADC"]
		df["CADC"] = df["CADC"] + df["CADC-Private"]
		df["Others"] = pd.DataFrame([tot_gbs, tot_events])[0] - df["CADC"] - df["NRC"] - df["CC"]
		df = df[["CADC","NRC","CC", "Others"]].T.reset_index()
		df.columns = ["Domains", "Size", "Events"]

		for j in ["Size", "Events"]:
			p = Donut(df, label = "Domains", values = j, title = "Total: {0:.0f} {1:s}".format(tot_gbs if j == "Size" else tot_events, "TB" if j == "Size" else "million files") )
			if i >= 2:
				p.add_layout( Title(text = "In {0:s}".format("Size" if j == "Size" else "Number of Files"), align = "center" ), "below" )
			if j == "Size":
				p.add_layout( Title(text = "Downloads" if i == 0 else "Uploads", align = "center"), "left" )
			i += 1
			plots.append(p)

	grid = gridplot(plots, ncols = 2, plot_width = 600, plot_height = 600)
	
	query_dates = {
		"size" : 0,
		"query" : {
	        "bool" : {
	        	"filter" : [
	            	{ "term" : { "service" : "transfer_ws" } },
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
			}
	    } 
	}
	res = conn.search(index = idx, body = query_dates)
	start = res["aggregations"]["start_date"]['value_as_string']
	end = res["aggregations"]["end_date"]['value_as_string']	

	return column(Div(text = "<h1><center>Data Transfer From {0:s} To {1:s}</center></h1>".format(re.match("(\d{4}-\d{2}-\d{2})T", start).group(1), re.match("(\d{4}-\d{2}-\d{2})T", end).group(1)), width = 600), grid)		

def fig5(conn, idx):
	iprange = {"132.246.194*":"CADC", "132.246.195*":"CADC", "132.246.217*":"CADC", "132.246*":"NRC+CADC", "192.168*":"CADC-Private", "206.12*":"CC"}
	service = ["data_ws", "vospace_ws"]
	method = ["get", "put"]
	i = 0
	plots = []
	for m in method:
		for j, s in enumerate(service):
			events = []
			for _ in iprange:
				query = {
					"size" : 0,
					"query" : {
				        "bool" : {
				        	"filter" : [
				            	{ "term" : { "service" : s } },
				            	{ "term" : { "method" : m } },
				            	{ "term" : { "success" : True } },
				            	{
						      		"query_string" : {
							        	"query" : "from:{}".format(_)
							        }
							    }
				            ]
					    }    
				    }
				}

				try:
					res = conn.search(index = idx, body = query)
					#res = scan(conn, index = idx, query = query, scroll = "30m", size = 500)
				except TransportError as e:
					print(e.info)
					raise

				events.append({iprange[_]:res["hits"]["total"] / 1e6})
					
			query_tot_events = {
				"size" : 0,
				"query" : {
			        "bool" : {
			        	"filter" : [
			            	{ "term" : { "service" : s } },
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
					}
	    		}
			}

			res = conn.search(index = idx, body = query_tot_events)

			tot_events = res["hits"]["total"] / 1e6
			start = res["aggregations"]["start_date"]['value_as_string']
			end = res["aggregations"]["end_date"]['value_as_string']

			df_events = pd.DataFrame.from_dict(events).sum().to_frame().T
			df = pd.concat([df_events], ignore_index = True)

			df["NRC"] = df["NRC+CADC"] - df["CADC"]
			df["CADC"] = df["CADC"] + df["CADC-Private"]
			df["Others"] = pd.DataFrame([tot_events])[0] - df["CADC"] - df["NRC"] - df["CC"]
			df = df[["CADC","NRC","CC", "Others"]].T.reset_index()
			df.columns = ["Domains", "Events"]

			p = Donut(df, label = "Domains", values = "Events", title = "Total Events: {:.2f} Millions".format(tot_events) )
			if i >= 2:
				p.add_layout( Title(text = "data_ws" if j == 0 else "vospace_ws", align = "center" ), "below" )
			if j == 0:
				p.add_layout( Title(text = "Downloads" if i == 0 else "Uploads", align = "center"), "left" )
			i += 1
			plots.append(p)
	grid = gridplot(plots, ncols = 2, plot_width = 600, plot_height = 600)

	return column(Div(text = "<h1>Number of data_ws and vospace_ws From {0:s} To {1:s}</h1>".format(re.match("(\d{4}-\d{2}-\d{2})T", start).group(1), re.match("(\d{4}-\d{2}-\d{2})T", end).group(1)), width = 1200), grid)	

def fig6(conn, idx):
	dom = ["CC", "CADC", "NRC", "Others"]
	method = ["get", "put"]

	clr = ["green", "purple", "blue", "orange"]
	plts = []
	w = 0.2

	for m in method:
		
		p = figure(plot_width = 1200, toolbar_location = "above", title = "{} Rate for Past Two Years".format("Download" if m == "get" else "Upload"))
		for i, d in enumerate(dom):
			df = pd.DataFrame()
			query = {
				"size" : 0,
				"query" : {
			        "bool" : {
			        	"filter" : [
			            	{ "term" : { "service" : "transfer_ws" } },
			            	{ "term" : { "method" : m } },
			            	{ "term" : { "success" : True } },
						    { "range" : { "@timestamp" : { "gte" : "now-2y"} } },
						    { "range" : { "time" : { "gt" : 0 } } },
		            		{ "range" : { "bytes" : { "gt" : 0 } } }
			            ]
				    }    
			    },
				"aggs" : {
					"permo" : {
						"date_histogram" : {
			                "field" : "@timestamp",
			                "interval" : "month",
			                "format" : "yyyy-MM" 
			            },
						"aggs": {
							"perct_rate" : { 
								"percentiles" : { 
									"script" : {
										"lang" : "painless",
										"inline" : "doc['bytes'].value / doc['time'].value" 
									}
								}
							}
						}
					}	
				}
			}

			if d == "CC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:206.12*" } })
			elif d == "CADC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:(192.168* OR 132.246.194* OR 132.246.195* OR 132.246.217*)" } })
			elif d == "NRC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:132.246*" } })
				query["query"]["bool"]["must_not"] = [ { "query_string" : { "query" : "from:(132.246.194* OR 132.246.195* OR 132.246.217*)" } } ]
			else:
				query["query"]["bool"]["must_not"] = [ { "query_string" : { "query" : "from:(132.246* OR 206.12* OR 192.168*)" } } ]			
			try:
				res = conn.search(index = idx, body = query)
			except TransportError as e:
				print(e.info)
				raise

			keys = ["5.0", "25.0", "50.0", "75.0", "95.0"]
			for _ in res["aggregations"]["permo"]["buckets"]:
				df = df.append( pd.DataFrame([[ _["perct_rate"]["values"][__] for __ in keys ]], columns = keys, index = [_["key_as_string"]]) )

			df = df.groupby(df.index).sum()	/ 1e3

			#

			x = np.array([_ for _ in range(len(df))])	

			p.vbar(x = x - 0.3 + w * i, top = df[keys[3]], bottom = df[keys[1]], width = w, legend = d, color = clr[i])
			# vertical line
			# p.segment(x - 0.3 + w * i, df[keys[0]], x - 0.3 + w * i, df[keys[4]], color = "black")
			# horizontal
			p.segment(x - 0.3 + w * i - w / 2, df[keys[2]], x - 0.3 + w * i + w / 2, df[keys[2]], color = "black")
			# p.segment(x - 0.3 + w * i - w / 2, df[keys[0]], x - 0.3 + w * i + w / 2, df[keys[0]], color = "black")
			# p.segment(x - 0.3 + w * i - w / 2, df[keys[4]], x - 0.3 + w * i + w / 2, df[keys[4]], color = "black")

		p.yaxis.axis_label = "Rate (MB/s)"

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

		plts.append(p)	

	return column(plts)

def fig7(conn, idx):

	dom = ["CC", "CADC", "NRC", "Others"]
	
	for i in range(24):
		print("now-{}M".format(i + 1))
		df = pd.DataFrame()
		for d in dom:
			query = {
				"size" : 0,
				"query" : {
			        "bool" : {
			        	"filter" : [
			            	{ "term" : { "service" : "transfer_ws" } },
			            	{ "term" : { "method" : "get" } },
			            	{ "term" : { "servlet" : "transferaction"}},
			            	{ "range" : { "@timestamp" : { "gte" : "now-{}M".format(i + 1), "lte" : "now-{}M".format(i) } } }
			            ],
				     	"must_not" : [
				            { "match_phrase" : { "path" : "/mnt/tmp/new" } }
				        ]
				    }           
			    },
				"aggs" : {
					"unq_file" : {
						"cardinality" : {
							"field" : "path.keyword",
							"precision_threshold" : 400000
						}
					}	
				}
			}

			if d == "CC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:206.12*" } })
			elif d == "CADC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:(192.168* OR 132.246.194* OR 132.246.195* OR 132.246.217*)" } })
			elif d == "NRC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:132.246*" } })
				query["query"]["bool"]["must_not"] = [ { "query_string" : { "query" : "from:(132.246.194* OR 132.246.195* OR 132.246.217*)" } } ]
			else:
				query["query"]["bool"]["must_not"] = [ { "query_string" : { "query" : "from:(132.246* OR 206.12* OR 192.168*)" } } ]			

			try:
				res = conn.search(index = idx, body = query)
			except TransportError as e:
				print(e.info)
				raise

			df = df.append(pd.DataFrame([[res["aggregations"]["unq_file"]["value"] / 1e6]], columns = ["Unique File"], index = [d]))

			if df.loc[d, "Unique File"] == 0:
				df = pd.DataFrame()
				continue

			query = {
				"size" : 0,
				"query" : {
			        "bool" : {
			        	"filter" : [
			            	{ "term" : { "service" : "transfer_ws" } },
			            	{ "term" : { "method" : "get" } },
			            	{ "term" : { "servlet" : "transferaction"}},
			            	{ "range" : { "@timestamp" : { "gte" : "now-{}M".format(i + 1), "lte" : "now-{}M".format(i) } } }
			            ],
				     	"must_not" : [
				            { "match_phrase" : { "path" : "/mnt/tmp/new" } }
				        ]
				    }           
			    },
			    "aggs" : {
			        "unq_dl" : {
			            "terms" : {
			                "field" : "path.keyword",
			                "size": df.loc[d, "Unique File"] * 1e6,
			                "execution_hint" : "global_ordinals_low_cardinality"
			            }
			            ,
			            "aggs" : {
			            	"unq_sum" : {
			            		"avg" : {
			            			"field" : "bytes"
			            		}
			            	}
			            }
			        }
			        ,
			        "sum_unique_size": {
			            "sum_bucket": {
			                "buckets_path": "unq_dl>unq_sum" 
			            }
			        }
			    }
			}

			if d == "CC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:206.12*" } })
			elif d == "CADC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:(192.168* OR 132.246.194* OR 132.246.195* OR 132.246.217*)" } })
			elif d == "NRC":
				query["query"]["bool"].setdefault("filter", []).append({ "query_string" : { "query" : "from:132.246*" } })
				query["query"]["bool"]["must_not"] = [ { "query_string" : { "query" : "from:(132.246.194* OR 132.246.195* OR 132.246.217*)" } } ]
			else:
				query["query"]["bool"]["must_not"] = [ { "query_string" : { "query" : "from:(132.246* OR 206.12* OR 192.168*)" } } ]

			try:
				res = conn.search(index = idx, body = query)
			except TransportError as e:
				print(e.info)
				raise

			df = df.append(pd.DataFrame([[ res["aggregations"]["sum_unique_size"]["value"] / (1024**4) ]], columns = ["Unique File Size"], index = [d]))
			df = df.groupby(df.index).sum()

		print(df)

	# df = df.groupby(df.index).sum()	
	# print(df)

def fig8(conn, idx):
	
	method = ["get", "put"]
	plts = []
	for m in method:
		df = pd.DataFrame()
		query = {
			"size" : 0,
			"query" : {
		        "bool" : {
		        	"filter" : [
		            	{ "term" : { "service" : "transfer_ws" } },
		            	{ "term" : { "method" : m } }
		            ]
			    }    
		    },
			"aggs" : {
				"permo" : {
					"date_histogram" : {
		                "field" : "@timestamp",
		                "interval" : "month",
		                "format" : "yyyy-MM" 
		            },
					"aggs": {
						"success" : { 
							"terms" : { 
								"field" : "success",
								"size" : 2
							}
						}
					}
				}	
			}
		}

		res = conn.search(index = idx, body = query)

		for _ in res["aggregations"]["permo"]["buckets"]:
			df = df.append( pd.DataFrame( [[__["doc_count"] / _["doc_count"]for __ in _["success"]["buckets"]]], columns = ["t", "f"], index = [_["key_as_string"]] ) )
		p = Bar(df, df.index, "f", legend = False, ylabel = "", title = "{} Failure Rate".format("Download" if m == "get" else "Upload"), plot_width = 1200)	
		plts.append(p)

	return column(plts)

##
# trying to match apache download to tomcat download
# giving up
#
def fig9(conn):
	df = pd.DataFrame()

	query = {
		"size" : 0,
		"query" : {
	        "bool" : {
	        	"filter" : [
	            	{ "term" : { "service" : "transfer_ws" } },
	            	{ "term" : { "method" : "get" } },
	            	#{ "term" : { "success" : True } },
	            	#{ "term" : { "servlet" : "transferaction" } },
	            	#{ "exists" : { "field" : "jobID"}}
	            ]
	            #,
	            # "must_not" : [
	            # 	{ "term" : { "userAgent" : "wget" }},
	            # 	{ "term" : { "path" : "preview" }}
	            # ]
		    }    
	    },
		"aggs" : {
			"permo" : {
				"date_histogram" : {
	                "field" : "@timestamp",
	                "interval" : "month",
	                "format" : "yyyy-MM" 
	            }
			}	
		}
	}

	res = conn.search(index = "logs-tomcat", body = query)

	for _ in res["aggregations"]["permo"]["buckets"]:
		df = df.append(pd.DataFrame( [[_["doc_count"]]], columns = ["tomcat"], index = [_["key_as_string"]] ))

	query = {
		"size" : 0,
		"query" : {
	        "bool" : {
	        	"filter" : [
	            	{ "term" : { "verb" : "get" } },
	            	{ "query_string" : { "query" : "request:(vospace OR data)" } }
	            ]
		    }    
	    },
		"aggs" : {
			"permo" : {
				"date_histogram" : {
	                "field" : "@timestamp",
	                "interval" : "month",
	                "format" : "yyyy-MM" 
	            }
			}	
		}
	}

	res = conn.search(index = "logs-apache", body = query)

	for _ in res["aggregations"]["permo"]["buckets"]:
		df = df.append(pd.DataFrame( [[_["doc_count"]]], columns = ["apache"], index = [_["key_as_string"]] ))

	df = df.groupby(df.index).sum()

	print(df)

if __name__ == "__main__":
	conn = Init(timeout = 900).connect()

	### fig1(conn, "delivery_history-*")

	#fig2(conn, "logs-tomcat")
	#fig3(conn, "logs-tomcat")
	#fig8(conn, "logs-tomcat")
	fig7(conn, "logs-tomcat")

	#fig5(conn, "delivery_history-*")