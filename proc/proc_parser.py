import re
import csv
import sys
import gzip
import time
from datetime import datetime
import os

csvOutput = True if "-csv" in sys.argv else False
jsonOutput = True if "-json" in sys.argv else False

if not (csvOutput or jsonOutput):
    print("at least specify one type of output, -csv or -json ...")
    sys.exit(0)

try:
    log = sys.argv[sys.argv.index("-f") + 1]
    try:
        f = open(log, "r")
        f.close()
    except FileNotFoundError:
        print("the input file specified by -f cannot be found ...")
        sys.exit(0)
except ValueError:
    print("missing input flag -f ...")
    sys.exit(0)
except IndexError:
    print("no file specified by -f ...")
    sys.exit(0)

output = []
ts = set()
with gzip.open(log, "rb") as fin:
	content = fin.readlines()
	for i, line in enumerate(content):
		j = 1
		out = []
		line = line.decode("utf-8").replace("\x00", "").replace("\\r","").replace("\\n", "")
		##
		# from John's config
		#
		if re.search("RemoteEventLogger", line) or re.search("LogEventsServlet", line):
			continue
		##
		# group(1): time
		# group(2): optional microsec
		# group(3): service
		# group(4): servlet
		# group(5): message after '-'
		#
		r = re.search("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?) ([\w ]*) \[[\w-]+\] INFO  (\w*)  - (.+)", line)
		if r:
			d = datetime.strptime(r.group(1), "%Y-%m-%d %H:%M:%S.%f")
			t = int(time.mktime(d.timetuple())) * 1000 + d.microsecond / 1000
			k = 1
			tmp = t
			while tmp in ts:
				tmp = t + k
				k += 1
			ts.add(tmp)
			t = tmp
			out.append("\"timestamp\":%i" % t)
			out.append("\"service\":\"%s\"" % r.group(3))
			out.append("\"servlet\":\"%s\"" % r.group(4))
			message = r.group(5)
			if re.search("^END:", message):
				while not re.search("(\{.*\})", message):
					nextmessage = content[i + j].decode("utf-8").replace("\x00", "").replace("\\r","").replace("\\n", "")
					message += nextmessage.strip("\n")
					j += 1		
				tmp = re.search("(\{.*\})", message).group(1)
				tmp = tmp.replace("true", "True")
				tmp = tmp.replace("false", "False")
				r = re.search("\"message\"\:\"(.*?)\"[,}]", tmp)
				if r:
					##
					# if there is more than 10 '.' together, we remove them
					#
					t = re.search("\.{10,}", r.group(1))
					if t:
						print("** find lots of '.' in %s" % log)
						msg = re.sub("\.{10,}", " ", r.group(1))
					else:
						msg = r.group(1)	
					tar = "\"message\":\"%s\"" % msg.replace("\"", "\'") + r.group(0)[-1]
					tmp = re.sub("\"message\":\"(.*?)\"[,}]", tar, tmp)
				r = re.search("\"path\":\"(.*?)\"[,}]", tmp)
				if r:
					path = r.group(1).replace("\"", "\'")
					tar = "\"path\":\"%s\"" % path + r.group(0)[-1]
					tmp = re.sub("\"path\":\"(.*?)\"[,}]", tar, tmp)
				tags = eval(tmp)
				for x in tags:
					out.append("\"%s\":\"%s\"" % (x, tags[x]))
			else:
				##
				# ignore phase:START, since the info is duplicated in phase:END
				#
				if re.search("^START:", message):
					continue
				out.append("\"message\":\'%s\'" % message)
			output.append("{" + ",".join(out) + "}\n")
				
des = os.path.basename(log)

if jsonOutput:
	with gzip.open(des+".json.gz" ,"wt") as fout:
		for line in output:
			fout.write(line)

if csvOutput:
	#with gzip.open(log+".csv.gz","wt") as fout:
	with open(des+".csv","w") as fout:
		colName = ["timestamp", "service", "servlet", "user", "success", "method", "from", "message", "path", "time", "jobID", "bytes"]
		w = csv.DictWriter(fout, fieldnames = colName, delimiter = '|')
		#w.writeheader()
		for line in output:
			w.writerow(eval(line))
