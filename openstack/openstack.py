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

proj_categ = {'androphot': 'Galaxies', 'astroai': 'Solar and Stellar', 'ATP_Mechanics': 'Instrumentation and Methods', 'ATP_Software': 'Instrumentation and Methods', 'aot': 'Instrumentation and Methods', 'cadc': 'CANFAR', 'moproc': 'CANFAR', 'HST_RW': 'CANFAR', 'ots': 'Cosmology', 'willott': 'CANFAR', 'CANFAR': 'CANFAR', 'CANFAROps': 'CANFAR', 'CANFAR_east': 'CANFAR', 'CANFAROps_east': 'CANFAR', 'canarie': 'CANFAR', 'cesharon': 'Galaxies', 'cfht': 'CANFAR', 'clauds': 'Galaxies', 'cyberska': 'CANFAR', 'dao': 'CANFAR', 'debris': 'Solar and Stellar', 'dragonfly': 'Galaxies', 'EAO': 'CANFAR', 'jsa': 'CANFAR', 'gpi': 'Earth and Planetary', 'JCMT_GBS': 'Solar and Stellar', 'karun': 'Galaxies', 'mwsynthesis': 'Solar and Stellar', 'ngvs': 'Galaxies', 'nugrid_admin': 'Solar and Stellar', 'nvulic': 'Galaxies', 'OSSOS_Worker': 'Earth and Planetary', 'pandas': 'Galaxies', 'pscicluna': 'Solar and Stellar', 'rpike': 'Earth and Planetary', 'UVic_KBOs': 'Earth and Planetary', 'scuba2': 'Solar and Stellar', 'seti': 'Earth and Planetary', 'shaimaaali': 'Cosmology', 'TAOSII': 'Earth and Planetary', 'thor': 'Solar and Stellar', 'uvgc': 'Solar and Stellar'}

es = 'http://elastic:cadcstats@206.12.59.36:9200'

class Init():
	def __init__(self, url = None, timeout = 120):
		self.timeout = timeout
		if not url:
			self.url = es
		if not requests.get(self.url):
			print("Connection incorrect!")
			exit(0)
	def connect(self):
		return Elasticsearch(self.url, timeout = self.timeout)

# openstack and condor cpu usage per category
def fig1(conn):
	reses = []
	plts = [Div(text = "<h1>CPU Usage per Category</h1>", width = 1200)]
	for idx in ["logs-openstack", "logs-condor"]:
		args = ["cpuhours", 1 / 24 / 365] if idx == "logs-openstack" else ["CumulativeSlotTime", 1 / 3600 / 24 / 365]
		query = {
			"query" : {
				"bool" : {
					 "must": [ 
				        { "range": { "@timestamp": { "gte": "2015-01-01", "lt": "2017-01-01" }}} 
				    ]
				}
			},
			"aggs" : {
				"per_proj" : {
					"terms" : {
			    		"field" : "{}.keyword".format( (lambda: "project" if idx == "logs-openstack" else "Project")() ),
			    		"size" : 100
					},
					"aggs" : {
						"coreyr" : {
							"sum" : {
								"script" : {
									"lang" : "painless",
									"inline" : "doc['{}'].value * {}".format( *args )
								}
							}
						}
					}
				}	
			}		
		}

		res = conn.search(index = idx, body = query)

		df = pd.DataFrame()

		for _ in res["aggregations"]["per_proj"]["buckets"]:
			proj = _["key"]
			coreyr = _["coreyr"]["value"]
			df = df.append(pd.DataFrame([[proj, coreyr]], columns = ["proj", "Core Year"]))
		
		if idx == "logs-openstack":
			df = df[df.proj != "CANFAR"]

		df["proj"] = df["proj"].str.replace("canfar-", "")
		df["proj"] = df["proj"].str.replace("-", "_")

		for k in proj_categ:
			df.loc[df.proj == k, "categ"] = proj_categ[k]
		
		df = df.groupby("categ").sum().reset_index()

		ttl = "Interactive" if idx == "logs-openstack" else "Batch"
		p = Donut(df, label = "categ", values = "Core Year", title = "{} VMs CPU Usage ({:.0f} core years) for 2015 and 2016".format(ttl, df["Core Year"].sum()), plot_width = 800, plot_height = 800)

		plts.append(p)

	output_file("fig1.html")
	show(column(plts))

# OpenStack Usage per CANFAR Project (2015-2016)
def fig2(idx, conn):
	query = {
		"query" : {
			"bool" : {
				 "must": [ 
			        { "range": { "@timestamp": { "gte": "2015-01-01", "lt": "2017-01-01" }}} 
			    ]
			}
		},
		"aggs" : {
			"per_proj" : {
				"terms" : {
		    		"field" : "project.keyword",
		    		"size" : 100
				},
				"aggs" : {
					"coreyr" : {
						"sum" : {
							"script" : {
								"lang" : "painless",
								"inline" : "doc['cpuhours'].value / 24 / 3600 "
							}
						}
					}
				}
			}	
		}		
	}	
		
	res = conn.search(index = idx, body = query)
	
	df = pd.DataFrame()	

	for _ in res["aggregations"]["per_proj"]["buckets"]:
		proj = _["key"]
		coreyr = _["coreyr"]["value"]
		df = df.append(pd.DataFrame([[proj, coreyr]], columns = ["proj", "Core Year"]))

	df = df[df.proj != "CANFAR"].set_index("proj").sort_values("Core Year")

	y = [_ for _ in range(len(df))]
	p = figure(plot_width = 1200, plot_height = 1200, toolbar_location = "above")
	p.hbar(y = y, left = 0, right = df["Core Year"], color = "Purple", height = 0.8)	
	p.xaxis[0].axis_label = "Core Years"
	d = dict(zip(y, df.index))
	p.yaxis[0].ticker = FixedTicker(ticks = y)
	p.yaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) +
	"""
    if (tick in dic) {
        return dic[tick]
    }
    else {
        return ''
    }""")
	p.yaxis[0].axis_label = "Projects"
	p.legend.location = "bottom_right"
	p.title.text = "OpenStack Usage per CANFAR Project (2015-2016)"
	output_file("fig2.html")
	show(p)

# Jobs CPU Usage and VM CPU Usage for Batch Processing (2015-2016)
def fig3(conn):

	query = {
		"query" : {
			"bool" : {
				"must" : [
					{ "term" : { "location.keyword" : "west"} },
					{ "term" : { "project.keyword" : "CANFAR"} },
					{ "range": { "@timestamp": { "gte": "2015-01-01", "lt": "2017-01-01" } } }
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
					"coreyr" : {
						"sum" : {
							"script" : {
								"lang" : "painless",
								"inline" : "doc['cpuhours'].value / 24 / 365"
							}
		            	}
					}
				}
			}	
		}		
	}

	res = conn.search(index = "logs-openstack", body = query)

	df = pd.DataFrame()

	for _ in res["aggregations"]["peryr"]["buckets"]:
		proj = _["key_as_string"]
		coreyr = _["coreyr"]["value"]
		df = df.append(pd.DataFrame([[proj, coreyr]], columns = ["proj", "cy_os"]))
	df = df.set_index("proj")
	query2 = {
		"query" : {
			"bool" : {
				"must" : [
					{ "range": { "@timestamp": { "gte": "2015-01-01", "lt": "2017-01-01" } } }
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
					"coreyr" : {
						"sum" : {
							"script" : {
								"lang" : "painless",
								"inline" : "doc['CumulativeSlotTime'].value / 3600 / 24 / 365"
							}
		            	}
					}
				}
			}	
		}		
	}

	res2 = conn.search(index = "logs-condor", body = query2)

	df2 = pd.DataFrame()

	for _ in res2["aggregations"]["peryr"]["buckets"]:
		proj = _["key_as_string"]
		coreyr = _["coreyr"]["value"]
		df2 = df2.append(pd.DataFrame([[proj, coreyr]], columns = ["proj", "cy_condor"]))
	df2 = df2.set_index("proj")	

	df = df.join(df2)		

	x = np.array([_ for _ in range(len(df))])

	p = figure(plot_width = 800, toolbar_location = "above")
	p.vbar(x = x - 0.2, bottom = 0, top = df["cy_condor"], width = 0.4, color = "blue", legend = "VMs")
	p.vbar(x = x + 0.2, bottom = 0, top = df["cy_os"], width = 0.4, color = "purple", legend = "Batch Processing")
	p.yaxis[0].axis_label = "Core Years"
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
	p.xaxis.major_label_orientation = np.pi/4
	p.yaxis[0].axis_label = "Core Years"
	p.legend.location = "top_left"
	p.title.text = "Jobs CPU Usage and VM CPU Usage for Batch Processing (2015-2016)"
	output_file("fig3.html")

	show(p)

# CPU Usage and Number of VMs for OpenStack Interactive
def fig4(idx, conn):
	query = {
		"query" : {
			"bool" : {
				"must" : [
					{ "range": { "@timestamp": { "gte": "2015-01-01", "lt": "2017-01-01" } } }
				],
				"must_not": [
					{ "term" : { "project.keyword" : "CANFAR"} }
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
					"coreyr" : {
						"sum" : {
							"script" : {
								"lang" : "painless",
								"inline" : "doc['cpuhours'].value / 24 / 365"
							}
		            	}
					},
					"nserver" : {
						"sum" : {
							"script" : {
								"lang" : "painless",
								"inline" : "doc['nserver'].value"
							}
		            	}
					}
				}
			}	
		}		
	}

	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()

	for _ in res["aggregations"]["peryr"]["buckets"]:
		yr = _["key_as_string"]
		cy = _["coreyr"]["value"]
		ns = _["nserver"]["value"]
		df = df.append(pd.DataFrame([[cy, ns]], columns = ["coreyr", "nserver"], index = [yr]))

	x = np.array([_ for _ in range(len(df))])
	p = figure(plot_width = 800, toolbar_location = "above")
	p.vbar(x = x - 0.2, top = df["coreyr"], bottom = 0, width = 0.4, legend = "CPU", color = "green")
	p.y_range = Range1d(0, df["coreyr"].max() * 1.1)
	p.extra_y_ranges = {"right_yaxis": Range1d(0, df["nserver"].max() * 1.1)}
	p.add_layout(LinearAxis(y_range_name = "right_yaxis", axis_label = "Number of VM Launched"), "right")
	p.yaxis.axis_label = "CPU Usage (core days)"
	p.vbar(x = x + 0.2, top = df["nserver"], bottom = 0, width = 0.4, legend = "Number of VMs", color = "purple", y_range_name = "right_yaxis")
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
	p.xaxis.major_label_orientation = np.pi / 4
	p.title.text = "CPU Usage and Number of VMs for OpenStack Interactive"
	output_file("fig4.html")
	show(p)

if __name__ == "__main__":
	conn = Init(timeout = 300).connect()
	#fig1(conn)
	#fig2("logs-openstack", conn)
	#fig3(conn)
	fig4("logs-openstack", conn)