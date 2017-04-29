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

es = 'http://users:cadcusers@206.12.59.36:9200'

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
def fig1(conn, idx, exclude = False):
	if exclude:
		query = {
			"size" : 0,
			"query" : {
				"bool" : {
					"must_not" : [
						{ "term" : { "remoteip" : "132.246.0.0/16"}},
						{ "term" : { "remoteip" : "206.12.0.0/16"}},
						{ "term" : { "remoteip" : "192.168.0.0/16"}},
						{ "term" : { "clientdomain.keyword" : "hawaii.edu" } }
					]
				}
			}
		}
	else:
		query = { "size" : 0 }	

	tot = conn.search(idx, body = query)["hits"]["total"]
	begin = conn.search(idx, body = { "aggs" : { "start" : { "min" :{ "field" : "@timestamp"}}}})["aggregations"]["start"]["value_as_string"][:10]
	end = conn.search(idx, body = { "aggs" : { "end" : { "max" :{ "field" : "@timestamp"}}}})["aggregations"]["end"]["value_as_string"][:10]

	df = pd.DataFrame([[tot]], index = ["tot"])

	for f in fields:
		if f == "data_release_date_public" or f == "target_upload":
			q = query.copy()	
			if exclude:
				q["query"]["bool"]["must"] = [ { "term" : {f : "true"}} ]
			else:
				q["query"] = {
					"bool" : {
						"must" : [ { "term" : {f : "true"}} ]
					}
				} 
			df = df.append(pd.DataFrame([[ conn.search(idx, body = q)["hits"]["total"] ]], index = [f]))
		elif f == "observation_intention":
			q = query.copy()
			q["aggs"] = {
				"obs_int": {
					"terms": {
						"field": "observation_intention.keyword",
						"size": 10
					}
				}
			}
			for _ in conn.search(idx, body = q)["aggregations"]["obs_int"]["buckets"]:
				if _["key"] != "both":
					df = df.append(pd.DataFrame([[ _["doc_count"] ]], index = [f + "_" + _["key"]]))
		else:
			q = query.copy()
			if exclude:
				q["query"]["bool"]["must"] = [ {"exists" : { "field" : f } } ]
			else:	
				q["query"] = {
					"bool" : {
						"must" : [ {"exists" : { "field" : f } } ]
					}
				} 
			df = df.append(pd.DataFrame([[ conn.search(idx, body = q)["hits"]["total"] ]], index = [f]))	

	df = df / tot
	ttl = "Advanced Search: Usage of Each Field (from %s to %s)" % (begin, end)
	if exclude:
		ttl += " excluding NRC/CADC/CC/Hawaii.edu Domain"
	p = figure(width = 1200, title = ttl)
	df = df.sort_values(0)
	y = [ _ for _ in range(1, len(df))]
	d = dict(zip(y, [_ for _ in df.index[:-1]]))
	p.hbar(y = y, right = df[0][:-1], height = 0.5, left = 0)
	xlab = "Percentage (Total Number of Queries %i)" % tot
	if exclude:
		xlab += " excluding NRC/CADC/CC/Hawaii.edu Domain"
	p.xaxis.axis_label = xlab
	p.yaxis.axis_label = "Fields"
	p.yaxis[0].ticker = FixedTicker(ticks = y)
	p.yaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) + """
	     return dic[tick]""")
	p.xaxis[0].formatter = NumeralTickFormatter(format = "0.%")

	return(p)

#
def fig2(conn, idx, exclude = False):
	query = {
		"size" : 0,
		"aggs" : {
			"peryr" : {
				"date_histogram" : {
		    		"field" : "@timestamp",
				    "interval" : "month",
				    "format" : "yyyy-MM" 
				}
			}	
		}
	}

	if exclude:
		query["query"] = {
			"bool" : {
				"must_not" : [
					{ "range" : { "remoteip" : { "gte" : "206.12.0.0", "lte" : "206.12.255.255" } } },
					{ "range" : { "remoteip" : { "gte" : "132.246.0.0", "lte" : "132.246.255.255" } } },
					{ "range" : { "remoteip" : { "gte" : "192.168.0.0", "lte" : "192.168.255.255" } } },
					{ "term" : { "clientdomain.keyword" : "hawaii.edu" } }
				]
			}
		}		

	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()
	
	for _ in res["aggregations"]["peryr"]["buckets"]:
		df = df.append(pd.DataFrame([[_["doc_count"]]], index = [_["key_as_string"]], columns = ["events"]))

	ttl = "Advanced Search: Numer of Queries per Month"
	if exclude:
		ttl += " excluding NRC/CADC/CC/Hawaii.edu Domain"
	p1 = figure(width = 1200, title = ttl)
	x = [_ for _ in range(len(df))]
	p1.vbar(x = x, top = df["events"], bottom = 0, width = 0.8)
	d = dict(zip(x, df.index))
	p1.xaxis[0].ticker = FixedTicker(ticks = x)
	p1.xaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) + """
	    if (tick in dic) {
	        return dic[tick]
	    }
	    else {
	        return ''
	    }""")
	p1.yaxis[0].axis_label = "Number of Queries"
	p1.xaxis.major_label_orientation = np.pi / 4

	return(p1)

def fig3(conn, idx):
	tot = conn.search(idx, body = { "query" : { "match_all" : {}}})["hits"]["total"]
	begin = conn.search(idx, body = { "aggs" : { "start" : { "min" :{ "field" : "@timestamp"}}}})["aggregations"]["start"]["value_as_string"][:10]
	end = conn.search(idx, body = { "aggs" : { "end" : { "max" :{ "field" : "@timestamp"}}}})["aggregations"]["end"]["value_as_string"][:10]

	query = {
		"size" : 0,
		"aggs" : {
			"perip" : {
				"terms" : {
		    		"field" : "remoteip",
				    "size" : 9
				}
			}	
		}
	}

	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()

	for _ in res["aggregations"]["perip"]["buckets"]:
		df = df.append(pd.DataFrame([[_["doc_count"]]], index = [_["key"]], columns = ["events"]))
	df = df.sort_values("events", ascending = True)
	df = pd.DataFrame([[res["aggregations"]["perip"]["sum_other_doc_count"]]], index = ["Others"], columns = ["events"]).append(df)

	p2 = figure(width = 900, title = "Advanced Search: Number of Queries submitted by IP (from %s to %s)" % (begin, end))
	y = [ _ for _ in range(len(df))]
	d = dict(zip(y, df.index))
	p2.hbar(y = y, right = df["events"] / tot, height = 0.8, left = 0)
	p2.xaxis.axis_label = ("Percentage (Total Number of Queries %i)" % tot)
	p2.yaxis.axis_label = "IP"
	p2.yaxis[0].ticker = FixedTicker(ticks = y)
	p2.yaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) + """
	     return dic[tick]""")
	p2.xaxis[0].formatter = NumeralTickFormatter(format = "0.%")

	return(p2)

def fig4(conn, idx):
	query = {
		"query" : {
			"bool" : {
				"must" : [
					{ "range" : { "@timestamp" : {"gte" : "2016-01", "lt" : "2016-02"} } }
				]
			}
		},
		"size" : 0,
		"aggs" : {
			"perip" : {
				"terms" : {
		    		"field" : "remoteip",
				    "size" : 4
				}
			}	
		}
	}	

	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()

	for _ in res["aggregations"]["perip"]["buckets"]:
		df = df.append(pd.DataFrame([[_["doc_count"]]], index = [_["key"]], columns = ["events"]))
	df = df.sort_values("events", ascending = True)
	df = pd.DataFrame([[res["aggregations"]["perip"]["sum_other_doc_count"]]], index = ["Others"], columns = ["events"]).append(df)

	p2 = figure(width = 900, title = "Number of Queries submitted in 2016-01 by IP")
	y = [ _ for _ in range(len(df))]
	d = dict(zip(y, df.index))
	p2.hbar(y = y, right = df["events"], height = 0.8, left = 0)
	p2.xaxis.axis_label = "Counts"
	p2.yaxis.axis_label = "IP"
	p2.yaxis[0].ticker = FixedTicker(ticks = y)
	p2.yaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) + """
	     return dic[tick]""")

	return(p2)

def fig5(conn, idx):
	query = {
		"size" : 0,
		"aggs" : {
	        "perday" : {
	            "date_histogram" : {
	                "field" : "@timestamp",
	                "interval" : "day"
	            }
	        }
	    }
	}

	res = conn.search(index = idx, body = query)

	print("Average query per day : {}".format(res["hits"]["total"] / len(res["aggregations"]["perday"]["buckets"])))

def fig6(conn, idx):

	begin = conn.search(idx, body = { "aggs" : { "start" : { "min" :{ "field" : "@timestamp"}}}})["aggregations"]["start"]["value_as_string"][:10]
	end = conn.search(idx, body = { "aggs" : { "end" : { "max" :{ "field" : "@timestamp"}}}})["aggregations"]["end"]["value_as_string"][:10]

	query = {
		"size" : 0,
		"query" : {
			"bool" : {
				"should" : [
					{ "range" : { "remoteip" : { "gte" : "132.246.194.0", "lte" : "132.246.194.255" } } },
					{ "range" : { "remoteip" : { "gte" : "132.246.195.0", "lte" : "132.246.195.255" } } }, 
					{ "range" : { "remoteip" : { "gte" : "132.246.217.0", "lte" : "132.246.217.255" } } },
					{ "range" : { "remoteip" : { "gte" : "192.168.0.0", "lte" : "192.168.255.255" } } } 
				]
			}
		},
		"aggs" : {
			"top_ips" : {
		        "terms" : {
		        	"field" : "remoteip",
		        	"size" : 4
		        }
		    } 
	    }
	}

	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()

	for _ in res["aggregations"]["top_ips"]["buckets"]:
		df = df.append(pd.DataFrame([["CADC", _["key"], _["doc_count"]]], columns = ["domain", "ip", "events"]))
	df = df.append(pd.DataFrame([["CADC", "rest", res["aggregations"]["top_ips"]["sum_other_doc_count"]]], columns = ["domain", "ip", "events"]))

	query = {
		"size" : 0,
		"query" : {
			"bool" : {
				"must" : [
					{ "range" : { "remoteip" : { "gte" : "206.12.0.0", "lte" : "206.12.255.255" } } }
				]
			}
		},
		"aggs" : {
			"top_ips" : {
		        "terms" : {
		        	"field" : "remoteip",
		        	"size" : 1
		        }
		    } 
	    }
	}

	res = conn.search(index = idx, body = query)

	for _ in res["aggregations"]["top_ips"]["buckets"]:
		df = df.append(pd.DataFrame([["CC", _["key"], _["doc_count"]]], columns = ["domain", "ip", "events"]))
	df = df.append(pd.DataFrame([["CC", "rest", res["aggregations"]["top_ips"]["sum_other_doc_count"]]], columns = ["domain", "ip", "events"]))

	query = {
		"size" : 0,
		"query" : {
			"bool" : {
				"must" : [
					{ "range" : { "remoteip" : { "gte" : "132.246.0.0", "lte" : "132.246.255.255" } } }
				],
				"must_not" : [
					{ "range" : { "remoteip" : { "gte" : "132.246.194.0", "lte" : "132.246.194.255" } } },
					{ "range" : { "remoteip" : { "gte" : "132.246.195.0", "lte" : "132.246.195.255" } } }, 
					{ "range" : { "remoteip" : { "gte" : "132.246.217.0", "lte" : "132.246.217.255" } } },
					{ "range" : { "remoteip" : { "gte" : "192.168.0.0", "lte" : "192.168.255.255" } } } 
				]
			}
		},
		"aggs" : {
			"top_ips" : {
		        "terms" : {
		        	"field" : "remoteip",
		        	"size" : 3
		        }
		    } 
	    }
	}

	res = conn.search(index = idx, body = query)

	for _ in res["aggregations"]["top_ips"]["buckets"]:
		df = df.append(pd.DataFrame([["NRC w/o CADC", _["key"], _["doc_count"]]], columns = ["domain", "ip", "events"]))
	df = df.append(pd.DataFrame([["NRC w/o CADC", "rest", res["aggregations"]["top_ips"]["sum_other_doc_count"]]], columns = ["domain", "ip", "events"]))

	query = {
		"size" : 0,
		"query" : {
			"bool" : {
				"must_not" : [
					{ "range" : { "remoteip" : { "gte" : "206.12.0.0", "lte" : "206.12.255.255" } } },
					{ "range" : { "remoteip" : { "gte" : "132.246.0.0", "lte" : "132.246.255.255" } } },
					{ "range" : { "remoteip" : { "gte" : "192.168.0.0", "lte" : "192.168.255.255" } } }
				]
			}
		},
		"aggs" : {
			"top_ips" : {
		        "terms" : {
		        	"field" : "remoteip",
		        	"size" : 4
		        }
		    } 
	    }
	}

	res = conn.search(index = idx, body = query)

	for _ in res["aggregations"]["top_ips"]["buckets"]:
		df = df.append(pd.DataFrame([["Others", _["key"], _["doc_count"]]], columns = ["domain", "ip", "events"]))
	df = df.append(pd.DataFrame([["Others", "rest", res["aggregations"]["top_ips"]["sum_other_doc_count"]]], columns = ["domain", "ip", "events"]))

	p = Donut(df, label = ["domain", "ip"], values = "events", title = "Advanced Search: Queries Percentage Submitted by Domains (from %s to %s)" % (begin, end), plot_width = 800, plot_height = 800)

	return(p)

def fig7(conn, idx, tgt):
	begin = conn.search(idx, body = { "aggs" : { "start" : { "min" :{ "field" : "@timestamp"}}}})["aggregations"]["start"]["value_as_string"][:10]
	end = conn.search(idx, body = { "aggs" : { "end" : { "max" :{ "field" : "@timestamp"}}}})["aggregations"]["end"]["value_as_string"][:10]

	query = {
		"size" : 0,
		"query" : {
			"bool" : {
				"must_not" : [
					{ "range" : { "remoteip" : { "gte" : "206.12.0.0", "lte" : "206.12.255.255" } } },
					{ "range" : { "remoteip" : { "gte" : "132.246.0.0", "lte" : "132.246.255.255" } } },
					{ "range" : { "remoteip" : { "gte" : "192.168.0.0", "lte" : "192.168.255.255" } } },
					{ "term" : { "clientdomain.keyword" : "hawaii.edu" } }
				]
			}
		},
		"aggs" : {
			"top_tgt" : {
		        "terms" : {
		        	"field" : "{}.keyword".format(tgt),
		        	"size" : 8 if tgt == "clientdomain" else 18
		        }
		    } 
	    }
	}

	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()

	for _ in res["aggregations"]["top_tgt"]["buckets"]:
		df = df.append(pd.DataFrame([[_["key"], _["doc_count"]]], columns = ["key", "events"]))
	df = df.sort_values("events")
	if tgt == "clientdomain":	
		df = pd.DataFrame([["Others", res["aggregations"]["top_tgt"]["sum_other_doc_count"]]], columns = ["key", "events"]).append(df)

	tot = res["hits"]["total"]

	df["events"] = df["events"] / tot

	if tgt == "collection":
		ttl = 'Most Selected "Collection" for Non-NRC/CADC/CC/Hawaii.edu ({0:.2f}% Selected "Collection")'.format(100 * df["events"].sum())
	else:
		ttl = "Advanced Search: Number of Queries submitted by Non-NRC/CADC/CC/Hawaii.edu Domains (from %s to %s)" % (begin, end)	
	p = figure(width = 900, title = ttl)
	y = [ _ for _ in range(len(df))]
	d = dict(zip(y, [_ for _ in df["key"]]))
	p.hbar(y = y, right = df["events"], height = 0.8, left = 0)
	p.xaxis.axis_label = "Percentage (Total Number of Queries Submitted by Non-NRC/CADC/CC/Hawaii.edu Domains: %i)" % tot
	p.yaxis.axis_label = "Collections" if tgt == "collection" else "Domains"
	p.yaxis[0].ticker = FixedTicker(ticks = y)
	p.yaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) + """
	     return dic[tick]""")
	p.xaxis[0].formatter = NumeralTickFormatter(format = "0.%")

	return(p)

if __name__ == "__main__":
	conn = Init(timeout = 1000).connect()
	#fig1("logs-advancedsearch", conn)
	#fig2("logs-advancedsearch", conn)
	#fig3("logs-advancedsearch", conn)
	#fig4("logs-advancedsearch", conn)
	#fig5("logs-advancedsearch", conn)
	fig6(conn, "logs-advancedsearch")
	#fig7("logs-advancedsearch", conn, "collection")
	#fig7("logs-advancedsearch", conn, "clientdomain")
	#fig1("logs-advancedsearch", conn, exclude = True)
	#fig2("logs-advancedsearch", conn, exclude = True)
		