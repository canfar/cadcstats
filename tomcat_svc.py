#!/Users/will/anaconda3/bin/python

from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import scan
import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
from ipaddress import IPv4Address as ipv4, AddressValueError
import time

odin = 'http://odin.cadc.dao.nrc.ca:9200'

class Conn():
	def __init__(self, url = None, timeout = 120):
		if not url:
			url = odin
		if not requests.get(url):
			print("Connection incorrect!")
			exit(0)
		self.conn = Elasticsearch(url, timeout = timeout)

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
	fig = plt.figure(figsize = (60, 40))
	method = ["PUT","GET"]
	service = ["transfer_ws", "data_ws", "vospace_ws"]
	p = 1
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
			ax = fig.add_subplot(3, 2, p)
			df.plot(kind = "pie", y = "doc_count", ax = ax, autopct = '%1.1f%%', labels = df["key"], legend = False, fontsize = "x-small")
			ax.set_title("service: {0}, method: {1}".format(s, m))
			ax.set_ylabel("")
			ax.axis('equal')
			p += 1
	plt.show()

def fig2(conn, idx):
	fig = plt.figure(figsize = (30, 10))
	service = ["transfer_ws", "data_ws", "vospace_ws"]
	method = ["GET", "PUT"]
	pos = [0, 1, -1]
	clr = ["blue", "purple", "green"]
	for j, s in enumerate(service):
		ax = fig.add_subplot(3, 1, j + 1)
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
			#print(res["aggregations"]["avgdur_perwk"]["buckets"])
			wk = [_["key_as_string"] for _ in res["aggregations"]["avgdur_perwk"]["buckets"]]
			avg_dur = [_["avgdur"]["value"] for _ in res["aggregations"]["avgdur_perwk"]["buckets"]]
			df = pd.DataFrame(list(zip(wk, avg_dur)), columns = ["time", "avg_dur"]).set_index("time")	
			df.plot(kind = "bar", ax = ax, width = 0.3, logy = True, position = pos[i], color = clr[i])
			ax.set_title("Average Duration over Time: service: {0}".format(service[j]))
			ax.set_ylabel("Average Duration")
			ax.set_xticklabels(ax.xaxis.get_ticklabels(), rotation = 45, size = "xx-small")
		ax.legend(method, loc = 2)
		ax.set_xticks([_ - 1 for _ in ax.get_xticks()])	
	plt.show()

def fig3(conn, idx):
	fig = plt.figure(figsize = (10, 5))
	ax = fig.add_subplot(111)
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
					    "interval" : "1W",
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
	avg_dur = [_["avgrate"]["value"] for _ in res["aggregations"]["avgrate_perwk"]["buckets"]]
	df = pd.DataFrame(list(zip(wk, avg_dur)), columns = ["time", "avg_dur"]).set_index("time")
	# load condor stats and use df's index
	df2 = pd.read_csv("/Users/will/Downloads/condor.csv")
	df2 = df2.set_index(df.index[:len(df2)])["Count"]
	#print(df2)
	df.plot(kind = "bar", ax = ax)
	ax.set_title('Average Rate over Time of "transfer_ws" from batch.canfar.net VS Number of Batching Jobs Completed')
	ax.set_ylabel("Average Rate")	
	ax.legend(["transfer_ws"], loc = 2)
	ax.set_xticklabels(ax.xaxis.get_ticklabels(), rotation = 45, size = "xx-small")
	ax.set_xticks([_ - 1 for _ in ax.get_xticks()])
	ax2 = ax.twinx()
	df2.plot(kind = "line", ax = ax2, color = "red")
	ax2.legend(["Batching jobs"], loc = 1)
	ax2.set_ylabel("Number of Batching Jobs Completed")
	plt.show()
	#print(ax.get_xticks())	

def fig4(conn, idx):
	
	iprange = {("132.246.194.0", "132.246.194.24"):"CADC", ("132.246.195.0", "132.246.195.24"):"CADC", ("132.246.217.0", "132.246.217.24"):"CADC", ("132.246.0.0", "132.246.255.255"):"NRC+CADC", ("192.168.0.0", "192.168.255.255"):"CADC-Private", ("206.12.0.0", "206.12.255.255"):"CC"}
	method = ["GET", "PUT"]
	fig = plt.figure()
	i = 0
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
		#print(tot_gbs, tot_events)

		q2 = {
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
				}
			}
		}	
		res = conn.search(index = idx, body = q2)
		start = res["aggregations"]["start_date"]['value_as_string']
		end = res["aggregations"]["end_date"]['value_as_string']

		df_gbs = pd.DataFrame.from_dict(gbs).sum().to_frame().T
		df_events = pd.DataFrame.from_dict(events).sum().to_frame().T
		#print(df_gbs)
		#print(df_events)
		df = pd.concat([df_gbs, df_events], ignore_index = True)
		#print(df)

		df["NRC"] = df["NRC+CADC"] - df["CADC"]
		df["CADC"] = df["CADC"] + df["CADC-Private"]
		df["Others"] = pd.DataFrame([tot_gbs, tot_events])[0] - df["CADC"] - df["NRC"] - df["CC"]
		df = df[["CADC","NRC","CC", "Others"]].T
		#print(df) 

		for j in range(2):
			ax = fig.add_subplot(2, 2, i + 1)
			df.plot(kind = "pie", y = j, ax = ax, autopct = '%1.1f%%', legend = False, labels = df.index)
			ax.set_title("Total: {0:.0f} {1:s}".format( (lambda: tot_gbs / 1024 if j == 0 else tot_events / 1e6)(), (lambda: "TB" if j == 0 else "million files")() ))
			# if i < 2:
			# 	ax.text(0, 1.5, "Data Transfer From {0:s} To {1:s}".format(re.match("(\d{4}-\d{2}-\d{2})T", start).group(1), re.match("(\d{4}-\d{2}-\d{2})T", end).group(1)), size = "large", ha = "center")	
			# else:
			if i >= 2:
				ax.text(0, -1.5, "In {0:s}".format((lambda: "Size" if j == 0 else "Number of Files")()), size = "large", horizontalalignment = "center")	
			if j == 0:
				ax.set_ylabel((lambda: "Downloads" if i == 0 else "Uploads")(), size = "large")	
			else:
				ax.set_ylabel("")	
			ax.axis('equal')
			i += 1
	plt.suptitle("Data Transfer From {0:s} To {1:s}".format(re.match("(\d{4}-\d{2}-\d{2})T", start).group(1), re.match("(\d{4}-\d{2}-\d{2})T", end).group(1)))	
	plt.show()		


def fig5(conn, idx):
	iprange = {("132.246.194.0", "132.246.194.24"):"CADC", ("132.246.195.0", "132.246.195.24"):"CADC", ("132.246.217.0", "132.246.217.24"):"CADC", ("132.246.0.0", "132.246.255.255"):"NRC+CADC", ("192.168.0.0", "192.168.255.255"):"CADC-Private", ("206.12.0.0", "206.12.255.255"):"CC"}
	service = ["data_ws", "vospace_ws"]
	method = ["GET", "PUT"]
	fig = plt.figure()
	i = 0
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

				#gbs.append({iprange[_]:res["aggregations"]["ip_ranges"]["buckets"][0]["gigabytes"]["value"]})
				events.append({iprange[_]:res["aggregations"]["ip_ranges"]["buckets"][0]["doc_count"]})
				
			#tot_gbs = res["aggregations"]["tot_giga"]["value"]
			tot_events = res["hits"]["total"]
			#print(s,m,tot_events)

			q2 = {
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
					}
				}
			}	
			res = conn.search(index = idx, body = q2)
			start = res["aggregations"]["start_date"]['value_as_string']
			end = res["aggregations"]["end_date"]['value_as_string']

			#df_gbs = pd.DataFrame.from_dict(gbs).sum().to_frame().T
			df_events = pd.DataFrame.from_dict(events).sum().to_frame().T
			#print(df_gbs)
			#print(df_events)
			df = pd.concat([df_events], ignore_index = True)
			#print(df)

			df["NRC"] = df["NRC+CADC"] - df["CADC"]
			df["CADC"] = df["CADC"] + df["CADC-Private"]
			df["Others"] = pd.DataFrame([tot_events])[0] - df["CADC"] - df["NRC"] - df["CC"]
			df = df[["CADC","NRC","CC", "Others"]].T
			#print(df) 

			ax = fig.add_subplot(2, 2, i + 1)
			df.plot(kind = "pie", y = 0, ax = ax, autopct = '%1.1f%%', legend = False, labels = df.index, fontsize = "small")
			if j == 0:
				ax.set_ylabel( (lambda: "Downloads" if i == 0 else "Uploads")() )
			else:
				ax.set_ylabel("")
			if i >= 2:
				ax.text(0, -1.2, (lambda: "data_ws" if j == 0 else "vospace_ws")(), ha = "center", size = "large")
			ax.set_title("Total Events: {}".format(tot_events))		
			ax.axis('equal')
			i += 1
	plt.suptitle("From {0:s} To {1:s}".format(re.match("(\d{4}-\d{2}-\d{2})T", start).group(1), re.match("(\d{4}-\d{2}-\d{2})T", end).group(1)))
	plt.tight_layout()	
	plt.show()	

if __name__ == "__main__":
	conn = Conn().conn
	# fig1(conn, "tomcat-svc-*")
	# fig2(conn, "tomcat-svc-*")
	# fig3(conn, "tomcat-svc-*")
	# fig4(conn, "delivery_history-*")
	fig5(conn, "delivery_history-*")