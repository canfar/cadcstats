#!/Users/will/anaconda3/bin/python

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


es = 'http://elastic:cadcstats@206.12.59.36:9200'

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

# histogram of job duration vs machine duration
def fig2(idx, conn):
	query = {
		"query" : {
			"bool" : {
				"must" : [
					{"term" : {"JobStatus.keyword" : "Completed"}}
				]
			}
		},
		"aggs" : {
			"jobdur_ranges" : {
				"range" : {
					"field" : "JobDuration",
					# ranges are [from, to)
					"ranges" : [
						{"to" : 10},
						{"from" : 10, "to" : 60},
						{"from" : 60, "to" : 600},
						{"from" : 600, "to" : 3600},
						{"from" : 3600, "to" : 18000},
						{"from" : 18000, "to" : 36000},
						{"from" : 36000, "to" : 180000},
						{"from" : 180000, "to" : 252000},
						{"from" : 252000}
					]
				}
			},
			"machdur_ranges" : {
				"range" : {
					"script" : {
						"lang" : "painless",
						"inline" : "doc['CompletionDate'].value - doc['QDate'].value"
					},
					# ranges are [from, to)
					"ranges" : [
						{"to" : 10},
						{"from" : 10, "to" : 60},
						{"from" : 60, "to" : 600},
						{"from" : 600, "to" : 3600},
						{"from" : 3600, "to" : 18000},
						{"from" : 18000, "to" : 36000},
						{"from" : 36000, "to" : 180000},
						{"from" : 180000, "to" : 252000},
						{"from" : 252000}
					]
				}
			}
		}
	}
	res = conn.search(index = idx, body = query)
	df = pd.DataFrame()
	cols = ["<10s", "10s~1m", "1m~10m", "10m~1h", "1h~5h", "5h~10h", "10h~50h", "50h~70h", ">70h"]
	for i in ["jobdur_ranges", "machdur_ranges"]:
		df = df.append(pd.DataFrame([[_["doc_count"] for _ in res["aggregations"][i]["buckets"]]], columns = cols, index = [i]))
	df = df.T
	p = figure(plot_width = 1200, toolbar_location = "above")
	clr = ["blue", "purple", "orange", "green"]
	x = np.array([_ for _ in range(len(df))])

	for i, col in enumerate(df.columns):
		p.vbar(x = x + i/5 - 0.10, top = np.sqrt(df[col]), bottom = 0, width = 0.2, color = clr[i], legend = "User" if col == "jobdur_ranges" else "Machine")

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

	output_file("fig2.html")
	show(column(Div(text = "<h1>Batch Processing Jobs: Machine and User Duration</h1>", width = 1200), p))	

# Median of Machine and User Batch Job Duration
def fig3(idx, conn):
	query = {
		"query" : {
			"bool" : {
				"must" : [
					{"term" : {"JobStatus.keyword" : "Completed"}}
				]
			}
		},
		"aggs" : {
			"dur_peryr" : {
				"date_histogram" : {
		    		"field" : "@timestamp",
				    "interval" : "year",
				    "format" : "yyyy" 
				},
				"aggs" : {
					"jobdur_outlier" : {
						"percentiles" : {
		                	"field" : "JobDuration" 
		            	}
					},
					"machdur_outlier" : {
						"percentiles" : {
		                	"script" : {
								"lang" : "painless",
								"inline" : "doc['CompletionDate'].value - doc['QDate'].value"
							}
		            	}
					}
				}
			}	
		}	
	}
	res = conn.search(index = idx, body = query)
	df = pd.DataFrame()
	for _ in res["aggregations"]["dur_peryr"]["buckets"]:
		yr = _["key_as_string"]
		machdur_med = _["machdur_outlier"]["values"]["50.0"]
		jobdur_med = _["jobdur_outlier"]["values"]["50.0"]
		df = df.append(pd.DataFrame([[jobdur_med / 60, machdur_med / 60]], columns = ["jobdur_med", "machdur_med"], index = [yr]))
	print(df)

	p = figure(plot_width = 1200, toolbar_location = "above")
	clr = ["blue", "purple", "orange", "green"]
	x = np.array([_ for _ in range(len(df))])

	for i, col in enumerate(df.columns):
		p.vbar(x = x + i/5 - 0.10, top = np.sqrt(df[col]), bottom = 0, width = 0.2, color = clr[i], legend = "User" if col == "jobdur_med" else "Machine")

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
	y = np.array([5, 30, 100, 400, 1600])
	p.yaxis[0].ticker = FixedTicker(ticks = np.sqrt(y))
	p.yaxis[0].formatter = FuncTickFormatter(code = """return (tick**2).toLocaleString("en-US", { minimumFractionDigits: 0 })""")
	p.yaxis.axis_label = "Median of Duration (Mins)"

	output_file("fig3.html")
	show(column(Div(text = "<h1>Median of Machine and User Batch Job Duration</h1>", width = 1200), p))	

# Histogram of User Job Duration / Machine Job Duration Ratio
def fig4(idx, conn):
	query = {
		"query" : {
			"bool" : {
				"must" : [
					{"term" : {"JobStatus.keyword" : "Completed"}}
				]
			}
		},
		"aggs": {
			"ratio" : {
				"histogram" : {
					"field" : "JobDuration",
					"interval" : 0.001,
					"script" : "_value / (doc['CompletionDate'].value - doc['QDate'].value)"
				}		
			}	
		}	
	}
	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()
	for _ in res["aggregations"]["ratio"]["buckets"]:
		df = df.append(pd.DataFrame([[_["doc_count"]]], columns = ["ratio"], index = ['{:.3f}'.format(_["key"])]))
	
	p = figure(plot_width = 1200, toolbar_location = "above")
	p.vbar(x = list(map(float, df.index.values)), top = df["ratio"], bottom = 0, width = 0.001)
	p.xaxis[0].formatter = NumeralTickFormatter(format = "0.00%")
	p.yaxis.axis_label = "Number of Events"

	output_file("fig4.html")
	show(column(Div(text = "<h1>Histogram of User Job Duration / Machine Job Duration Ratio</h1>", width = 1200), p))		
		
# Number of Batch Processing Users
def fig5(idx, conn):
	query = {
		"query" : {
			"match_all" : {}
		},
		"aggs" : {
			"usr_peryr" : {
				"date_histogram" : {
		    		"field" : "@timestamp",
				    "interval" : "year",
				    "format" : "yyyy" 
				},
				"aggs" : {
					"unique_users" : {
						"cardinality" : {
							"field": "Owner.keyword"
						}
					}
				}
			}	
		}		
	}
	res = conn.search(index = idx, body = query)

	df = pd.DataFrame()
	for _ in res["aggregations"]["usr_peryr"]["buckets"]:
		yr = _["key_as_string"]
		val = _["unique_users"]["value"]
		df = df.append(pd.DataFrame([[val]], columns = ["uniq_usr"], index = [yr]))
	p = figure(plot_width = 1200, toolbar_location = "above")
	x = [_ for _ in range(len(df))]
	p.vbar(x = x, top = df["uniq_usr"], bottom = 0, width = 0.8)
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
	p.yaxis[0].axis_label = "Number of Users"
	p.xaxis[0].axis_label = "Year"
	output_file("fig5.html")
	show(column(Div(text = "<h1>Number of Batch Processing Users</h1>", width = 1200), p))	

# Request Ram/Dsk vs VM Ram/Dsk per VM Flavor
def fig6(idx, conn):
	query = {
		"query" : {
			"bool" : {
				"must_not" : [
					{ "term": { "VMInstanceType.keyword" : "c4.med"} },
					{ "term": { "VMInstanceType.keyword" : "12345678-6341-470e-92b7-5142014e7c5e"}},
					{ "term": { "VMInstanceType.keyword" : "5c1ed3eb-6341-470e-92b7-5142014e7c5e"}}
				]
			}
		},
		"aggs" : {
			"grpby_vm" : {
				"terms" : {
		    		"field" : "VMInstanceType.keyword",
		    		"size" : 100
				},
				"aggs" : {
					"avg_dskreq" : {
						"avg": {
							"field" : "RequestDisk"
						}
					},
					"avg_ramreq" : {
						"avg": {
							"field" : "RequestMemory"
						}
					},
					"dskspec" : {
						"avg": {
							"field" : "VMSpec.DISK"
						}
					},
					"ramspec" : {
						"avg": {
							"field" : "VMSpec.RAM"
						}
					}
				}
			}	
		}		
	}
	res = conn.search(index = idx, body = query)
	
	df = pd.DataFrame()
	for _ in res["aggregations"]["grpby_vm"]["buckets"]:
		vm = _["key"]
		avg_dskreq = _["avg_dskreq"]["value"]
		avg_ramreq = _["avg_ramreq"]["value"]
		dskspec = _["dskspec"]["value"]
		ramspec = _["ramspec"]["value"]
		df = df.append(pd.DataFrame([[vm, avg_dskreq / 1024, avg_ramreq, dskspec, ramspec]], columns = ["vm", "avg_dskreq", "avg_ramreq", "dskspec", "ramspec"]))

	VMAlias = {
    "c16.med":"13efd2a1-2fd8-48c4-822f-ce9bdc0e0004",
    "c2.med":"23090fc1-bdf7-433e-9804-a7ec3d11de08",
    "p8-12gb":"2cb70964-721d-47ff-badb-b702898b6fc2",
    "c4.hi":"5112ed51-d263-4cc7-8b0f-7ef4782f783c",
    "c2.low":"6c1ed3eb-6341-470e-92b7-5142014e7c5e",
    "c8.med":"72009191-d893-4a07-871c-7f6e50b4e110",
    "c4.low":"8061864c-722b-4f79-83af-91c3a835bd48",
    "p8-6gb":"848b71a2-ae6b-4fcf-bba4-b7b0fccff5cf",
    "c8.low":"8953676d-def7-4290-b239-4a14311fbb69",
    "c8.hi":"a55036b9-f40c-4781-a293-789647c063d7",
    "c16.hi":"d816ae8b-ab7d-403d-ae5f-f457b775903d",
    "p1-0.75gb-tobedeleted":"f9f6fbd7-a0af-4604-8911-041ea6cbbbe4"
	}
	df = df.replace({"vm": VMAlias})
	df = df.set_index("vm")
	df = df.groupby(df.index).mean().sort_values(by = "ramspec")

	y = np.array([_ for _ in range(len(df))])
	clr = ["purple", "blue", "green" , "orange"]
	w = 0.4

	p = figure(plot_width = 1200, toolbar_location = "above")
	for i, c in enumerate(["ramspec", "avg_ramreq"]):
		p.hbar(y = y - w * (i - 1 / 2) , right = df[c] / 1024, left = 0, height = w, color = clr[i], legend = "Requested Memory" if i == 1 else "VM Memory")
	p.xaxis[0].axis_label = "GB"
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
	p.yaxis[0].axis_label = "VM UUID"
	p.legend.location = "bottom_right"

	df = df.sort_values(by = "dskspec")

	p2 = figure(plot_width = 1200, toolbar_location = "above")
	for i, c in enumerate(["dskspec", "avg_dskreq"]):
		p2.hbar(y = y - w * (i - 1 / 2) , right = df[c], left = 0, height = w, color = clr[i], legend = "Requested Disk Size" if i == 1 else "VM Disk Size")
	p2.xaxis[0].axis_label = "GB"
	d = dict(zip(y, df.index))
	p2.yaxis[0].ticker = FixedTicker(ticks = y)
	p2.yaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) +
	"""
    if (tick in dic) {
        return dic[tick]
    }
    else {
        return ''
    }""")
	p2.yaxis[0].axis_label = "VM UUID"
	p2.legend.location = "bottom_right"

	output_file("fig6.html")
	show(column(Div(text = "<h1>Average Memory Requested For Batch VMS</h1>", width = 1200), p, Div(text = "<h1>Average Disk Requested For Batch VMS</h1>", width = 1200), p2))		

# Number of Jobs Completed, Disk Usage, Memory Usage per VM Ins per Year
def fig7(idx, conn):
	query = {
		"query" : {
			"bool" : {
				"must" : [
					{ "term": { "JobStatus.keyword" : "Completed"} }
				]
			}
		},
		"aggs" : {
			"peryr" : {
				"date_histogram" : {
		    		"field" : "@timestamp",
				    "interval" : "year",
				    "format" : "yyyy" 
				},
				"aggs" : {
					"tot_ram" : {
						"sum" : {
							"field" : "MemoryUsage"
						}
					},
					"tot_dsk" : {
						"sum" : {
							"field" : "DiskUsage"
						}
					},
					"vm_ins" : {
						"cardinality" : {
							"field" : "VMInstanceName.keyword"
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
		jobs_per_ins = _["doc_count"] / _["vm_ins"]["value"]
		ram_per_ins = _["tot_ram"]["value"] / _["vm_ins"]["value"] / 1024
		dsk_per_ins = _["tot_dsk"]["value"] / _["vm_ins"]["value"] / 1024
		df = df.append(pd.DataFrame([[jobs_per_ins, ram_per_ins, dsk_per_ins]], columns = ["jobs", "ram", "dsk"], index = [yr]))

	plts = [Div(text = "<h1>Basic Stats</h1>", width = 1200)]
	clr = ["blue", "purple", "orange", "green"]
	ylabs = ["", "GB", "GB"]
	ttl = ["Number of Jobs Completed", "Disk Usage", "Memory Usage"]
	x = [_ for _ in range(len(df))]
	for i in range(len(df.columns)):
		p = figure(plot_width = 800, toolbar_location = "above")
		p.vbar(x = x, top = df.ix[:,i], bottom = 0, width = 0.8, color = clr[i])
		p.title.text = "{} per VM Instance".format(ttl[i])
		p.yaxis[0].axis_label = ylabs[i]
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
		plts.append(p)

	output_file("fig7.html")
	show(column(plts))

def fig8(idx, conn):
	reses = []
	for _ in [("lt", "RequestMemory"),("gte", "VMSpec.RAM")]:
		query = {
			"query" : {
				"bool" : {
					"must" : [
						{ "term": { "JobStatus.keyword" : "Completed"} }, 
				        { "range": { "@timestamp": { _[0]: "2015-01-01" }}} 
				    ]
				}
			},
			"aggs" : {
				"peryr" : {
					"date_histogram" : {
			    		"field" : "@timestamp",
					    "interval" : "year",
					    "format" : "yyyy" 
					},
					"aggs" : {
						"med_reqmem" : {
							"percentiles" : {
								"field": "{}".format(_[1])
			            	}
						},
						"med_ratio" : {
							"percentiles" : {
			                	"script" : {
									"lang" : "painless",
									"inline" : "doc['MemoryUsage'].value / doc['{}'].value".format(_[1])
								}
			            	}
						}
					}
				}	
			}		
		}

		res = conn.search(index = idx, body = query)
		reses.append(res)
	
	df = pd.DataFrame()
	for __ in reses:
		for _ in __["aggregations"]["peryr"]["buckets"]:
			yr = _["key_as_string"]
			med_ratio = _["med_ratio"]["values"]["50.0"]
			med_reqmem = _["med_reqmem"]["values"]["50.0"]
			df = df.append(pd.DataFrame([[med_reqmem / 1024, med_ratio]], columns = ["med_mem", "med_ratio"], index = [yr]))

	plts = []
	clr = ["blue", "purple", "orange", "green"]
	ylabs = ["GB", ""]
	ttl = ["Requested Memory", "Memory Usage / Requested Memory Ratio"]
	x = [_ for _ in range(len(df))]
	for i in range(len(df.columns)):
		p = figure(plot_width = 800, toolbar_location = "above", y_axis_type = "log")
		if i == 1:
			p.y_range = Range1d(0.001, 1.1) 
		p.vbar(x = x, top = df.ix[:,i], bottom = 0, width = 0.8, color = clr[i])
		p.title.text = "Median of {} for Batch Jobs".format(ttl[i])
		p.yaxis[0].axis_label = ylabs[i]
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
		plts.append(p)

	output_file("fig8.html")
	show(column(plts))

def fig9(idx, conn):
	reses = []
	for _ in [("lt", "RequestMemory"),("gte", "VMSpec.RAM")]:
		query = {
			"query" : {
				"bool" : {
					"must" : [
						{ "term": { "JobStatus.keyword" : "Completed"} },
				        { "range": { "@timestamp": { _[0]: "2015-01-01" }}} 
				    ]
				}
			},
			"aggs" : {
				"per_proj" : {
					"terms" : {
			    		"field" : "Project.keyword",
			    		"size" : 100
					},
					"aggs" : {
						"memusg" : {
							"avg" : {
								"field": "MemoryUsage"
			            	}
						},
						"reqmem" : {
							"avg" : {
			                	"field" : "{}".format(_[1])
			            	}
						}
					}
				}	
			}		
		}

		res = conn.search(index = idx, body = query)
		reses.append(res)

	df = pd.DataFrame()
	for __ in reses:
		for _ in __["aggregations"]["per_proj"]["buckets"]:
			proj = _["key"]
			reqmem = _["reqmem"]["value"]
			memusg = _["memusg"]["value"]
			df = df.append(pd.DataFrame([[proj, reqmem / 1024, memusg / 1024]], columns = ["proj", "reqmem", "memusg"]))

	df = df.groupby("proj").sum().sort_values("reqmem")

	y = np.array([_ for _ in range(len(df))])
	clr = ["purple", "orange"]
	w = 0.4

	p = figure(plot_width = 800, toolbar_location = "above")
	for i, c in enumerate(["reqmem", "memusg"]):
		p.hbar(y = y - w * (i - 1 / 2) , right = df[c], left = 0, height = w, color = clr[i], legend = "Requested Memory" if i == 0 else "Memory Usage")
	p.xaxis[0].axis_label = "GB"
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

	output_file("fig9.html")
	show(column(Div(text = "<h1>Average Memory Usage & Requested Memory for Batch VMs</h1>", width = 1200), p))

def fig10(conn):
	df = pd.DataFrame()
	for idx in ["logs-tomcat", "logs-condor"]:
		query = {
			"size" : 0,
			"query" : {
				"bool" : {
					"must" : [
						{ "term" : { "service" : "proc_ws" } },
						{ "term" : { "method" : "post" } }
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
		            "aggs" : {
						"unique_user" : {
							"cardinality" : {
								"field": "user.keyword"
							}
						}	
					}
				}	
			}		
		}

		if idx == "logs-condor":
			query["query"] = { "match_all" : {} }
			query["aggs"]["permo"]["aggs"]["unique_user"]["cardinality"]["field"] = "Owner.keyword"

		res = conn.search(index = idx, body = query)

		for _ in res["aggregations"]["permo"]["buckets"]:
			df = df.append(pd.DataFrame([[_["unique_user"]["value"]]], columns = [idx], index = [_["key_as_string"]]))
	
	df = df.groupby(df.index).sum().dropna()
	df.columns = ["HTCondor", "Web Service"]

	x = np.array([_ for _ in range(len(df))])
	p = figure(plot_width = 800, toolbar_location = "above")
	p.vbar(x = x - 0.2, top = df["HTCondor"], bottom = 0, width = 0.4, legend = "HTCondor", color = "purple")
	p.vbar(x = x + 0.2, top = df["Web Service"], bottom = 0, width = 0.4, legend = "Web Service", color = "blue")
	p.legend.location = "top_right"
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
	p.title.text = "Users Submitting Jobs by Web Service and Directly by HTCondor"
	output_file("fig10.html")
	show(p)	

def fig11(conn):
	df = pd.DataFrame()
	for m in ["post", "get"]:
		query = {
			"size" : 0,
			"query" : {
				"bool" : {
					"must" : [
						{ "term" : { "service" : "proc_ws" } },
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
		            }
				}	
			}		
		}

		res = conn.search(index = "logs-tomcat", body = query)

		for _ in res["aggregations"]["permo"]["buckets"]:
			df = df.append(pd.DataFrame([[_["doc_count"]]], columns = [m], index = [_["key_as_string"]]))
	
	df = df.groupby(df.index).sum().dropna()
	df.columns = ["Job Submission", "Job Status"]

	x = np.array([_ for _ in range(len(df))])
	p = figure(plot_width = 800, toolbar_location = "above")
	p.vbar(x = x - 0.2, top = df["Job Submission"], bottom = 0, width = 0.4, legend = "Job Submission", color = "purple")
	p.vbar(x = x + 0.2, top = df["Job Status"], bottom = 0, width = 0.4, legend = "Job Status", color = "blue")
	p.legend.location = "top_right"
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
	p.title.text = "Requests for Job Queue Status and Job Submission"
	output_file("fig11.html")
	show(p)	

def fig12(conn):
	df = pd.DataFrame()
	for m in ["post", "get"]:
		query = {
			"size" : 0,
			"query" : {
				"bool" : {
					"must" : [
						{ "term" : { "service" : "proc_ws" } },
						{ "term" : { "method" : m } }
					]
				}
			},
			"aggs" : {
				"dur_hist" : {
		            "histogram" : {
		                "field" : "time",
		                "interval" : 100,
		                "extended_bounds" : {
		                    "min" : 0,
		                    "max" : 40000
		                }
		            }
		        }	
			}		
		}

		res = conn.search(index = "logs-tomcat", body = query)

		for _ in res["aggregations"]["dur_hist"]["buckets"]:
			df = df.append(pd.DataFrame([[_["doc_count"]]], columns = [m], index = [_["key"]]))
	
	df = df.groupby(df.index).sum().dropna()
	df.columns = ["Job Submission", "Job Status"]

	x = np.array([_ for _ in range(len(df))])
	p = figure(plot_width = 1200, toolbar_location = "above", y_axis_type = "log")
	p.vbar(x = x - 0.2, top = df["Job Submission"], bottom = 0, width = 0.4, legend = "Job Submission", color = "purple")
	p.vbar(x = x + 0.2, top = df["Job Status"], bottom = 0, width = 0.4, legend = "Job Status", color = "blue")
	p.legend.location = "top_right"
	p.title.text = "Web Service Requests for Job Status and Job Submission"
	output_file("fig12.html")
	show(p)

def fig13(conn):
	df = pd.DataFrame()

	query = {
		"size" : 0,
		"query" : {
			"bool" : {
				"must" : [
					{ "term" : { "service" : "proc_ws" } },
					{ "term" : { "method" : "post" } }
				],
				"must_not" : { "exists" : { "field" : "message" } }
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
		df = df.append(pd.DataFrame([[_["doc_count"]]], columns = ["proc_ws"], index = [_["key_as_string"]]))	

	query = {
		"size" : 0,
		"query" : {
			"bool" : {
				"must" : [
					{ "range" : { "ClusterId" : { "gte" : 0 } } },
					{ "range" : { "ProcId" : { "gte" : 0 } } }
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
	            "aggs" : {
	            	"uniq_clusterid" : {
	            		"cardinality" : {
	            			"field" : "ClusterId"
	            		}
	            	}
	            }
			}	
		}		
	}	

	res = conn.search(index = "logs-condor", body = query)

	for _ in res["aggregations"]["permo"]["buckets"]:
		df = df.append(pd.DataFrame([[_["uniq_clusterid"]["value"]]], columns = ["condor"], index = [_["key_as_string"]]))	

	df = df.groupby(df.index).sum()
	
	df["ratio"]	= df["proc_ws"] / df["condor"]

	df = df.dropna(how = "any")

	p1 = figure(width = 800, title = "Average Ratio of Web Service submissions over HTCondor Submissions")
	x = [_ for _ in range(len(df))]
	p1.vbar(x = x, top = df["ratio"], bottom = 0, width = 0.8)
	d = dict(zip(x, df.index))
	p1.xaxis[0].ticker = FixedTicker(ticks = x)
	p1.xaxis[0].formatter = FuncTickFormatter(code = """dic = """ + str(d) + """
	    if (tick in dic) {
	        return dic[tick]
	    }
	    else {
	        return ''
	    }""")
	p1.yaxis[0].axis_label = "Ratio"
	p1.xaxis.major_label_orientation = np.pi / 4
	output_file("fig13.html")
	show(p1)

if __name__ == "__main__":
	conn = Init(timeout = 300).connect()
	#fig1("logs-condor", conn)	
	#fig2("logs-condor", conn)
	#fig3("logs-condor", conn)
	#fig4("logs-condor", conn)
	#fig5("logs-condor", conn)
	#fig6("logs-condor", conn)	
	#fig7("logs-condor", conn)
	#fig8("logs-condor", conn)
	#fig9("logs-condor", conn)
	#fig10(conn)
	#fig11(conn)
	#fig12(conn)
	fig13(conn)
	#test()
