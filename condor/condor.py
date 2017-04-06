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


es = 'http://206.12.59.36:9200'

class Conn():
	def __init__(self, url = None, timeout = 120):
		if not url:
			url = es
		if not requests.get(url):
			print("Connection incorrect!")
			exit(0)
		self.conn = Elasticsearch(url, timeout = timeout)

# Number of Batch Jobs Restarts
def fig1(idx, conn):
	query = {
		"query" : {
			"match_all" : {}
		},
		"aggs" : {
			"numres_peryr" : {
				"date_histogram" : {
		    		"field" : "@timestamp",
				    "interval" : "year",
				    "format" : "yyyy" 
				},
				"aggs" : {
					"res_ranges" : {
						"range" : {
							"field" : "NumJobStarts",
							# ranges are [from, to)
							"ranges" : [
								{"to" : 1},
								{"from" : 1, "to" : 2},
								{"from" : 2, "to" : 6},
								{"from" : 6}
							]
						}
					}
				}
			}
		}
	}
	res = conn.search(index = idx, body = query)
	df = pd.DataFrame()

	for _ in res["aggregations"]["numres_peryr"]["buckets"]:
		yr = _["key_as_string"]
		events = [__["doc_count"] for __ in _["res_ranges"]["buckets"]]
		df = df.append(pd.DataFrame([events], columns = ["Never", "Once", "2-5", ">5"], index = [yr]))

	p = figure(plot_width = 1200, toolbar_location = "above")
	clr = ["blue", "purple", "orange", "green"]
	x = np.array([_ for _ in range(len(df))])

	for i, col in enumerate(df.columns):
		p.vbar(x = x + i/5 - 0.3, top = np.sqrt(df[col]), bottom = 0, width = 0.15, legend = col, color = clr[i])

	d = dict(zip(x, df.index))
	p.xaxis[0].ticker = FixedTicker(ticks = x)
	p.xaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) +
	"""
    if (tick in dic) {
        return dic[tick]
    }
    else {
        return ''
    }""")
	y = np.array([0.1, 0.5, 1, 2, 4])
	p.yaxis[0].ticker = FixedTicker(ticks = np.sqrt(y * 1e6))
	p.yaxis[0].formatter = FuncTickFormatter(code = """return (tick**2 / 1e6).toLocaleString("en-US", { minimumFractionDigits: 1 })""")
	p.yaxis.axis_label = "Number of jobs (millions)"

	output_file("fig1.html")
	show(column(Div(text = "<h1>Batch Processing Job Restarts</h1>", width = 600), p))	

if __name__ == "__main__":
	conn = Conn(timeout = 300).conn
	fig1("logs-condor", conn)		