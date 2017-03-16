from elasticsearch import Elasticsearch, TransportError
import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# check connection
if not requests.get('http://odin.cadc.dao.nrc.ca:9200'):
	print("Connection incorrect!")

conn = Elasticsearch('http://odin.cadc.dao.nrc.ca:9200')

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
			    	"domains": {
			    		"terms": {"field": "clientdomain", "size": 8}
			    	}

			    }
			}

		try:
			res = conn.search(index = "tomcat-svc-*", body = query)
		except 	TransportError as e:
			print(e.info)
			exit(0)

		df = pd.DataFrame.from_dict(res["aggregations"]["domains"]["buckets"])

		_ = pd.DataFrame([res["aggregations"]["domains"]["sum_other_doc_count"], "Others"]).T
		_.columns = df.columns

		df = df.append(_, ignore_index = True)

		#print(df)

		ax = fig.add_subplot(3, 2, p)

		df.plot(kind = "pie", y = "doc_count", ax = ax, autopct = '%1.1f%%', labels = df["key"], legend = False, fontsize = "x-small")
		ax.set_title("service: {0}, method:{1}".format(s, m))
		ax.set_ylabel("")
		ax.axis('equal')
		p += 1

plt.show()