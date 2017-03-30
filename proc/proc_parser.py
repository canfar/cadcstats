import re
import csv
import sys
import gzip
import time
from datetime import datetime
import os

colName = ["timestamp", "service", "servlet", "user", "success", "method", "from", "message", "path", "time", "jobID", "bytes", "target"]

csvOutput = True if "-csv" in sys.argv else False
jsonOutput = True if "-json" in sys.argv else False

if not (csvOutput or jsonOutput):
	print("at least specify one type of output, -csv or -json ...")
	sys.exit(0)

redo_mode = False
#redo_mode_write = False
if "-redo" in sys.argv:
	redo_mode = True
	redo_file = sys.argv[sys.argv.index("-redo") + 1]
	path = redo_file.split("_")
	log = "/data/tomcat/" + path[0] + "/archive/" + path[1] + "/" + ".".join(redo_file.split(".")[:-1])
	with open(redo_file, "r") as fin:
		# this list of line int will always be sorted, since the lines were sequentially read and written
		redo_lines = [int(_) for _ in fin.read().strip().split()]

if not redo_mode:
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

des = os.path.basename(log)
regex = re.compile(b'[\x00-\x1f]')
line_regex = re.compile("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?) ([\w ]*) \[.+?\] INFO  (\w*)  - (.+)")
msg_regex = re.compile("\"message\"\:\"(.*?)\"}$")
path_regex = re.compile("\"path\":\"(.*?)(\",(?=\")|\"}$)")
wtf_regex = re.compile('","}$')
wtf2_regex = re.compile('",","')
ts = set()

j = 0
with gzip.open(log, "rb") as fin, open(des + ".err", "w") as errout:
	for i, line in enumerate(fin):
		# if redo_mode_write:
		# 	break
		if redo_mode:
			if j >= len(redo_lines):
				break
			line_num = redo_lines[j]
			if i != line_num:
				continue
			else:
				j += 1	
		out = []
		line = regex.sub(b"", line).decode("utf-8").strip().replace("\\r", "").replace("\\n", "").replace("\\u0000", "")
		line = wtf_regex.sub('"}', line)
		line = wtf2_regex.sub('","', line)
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
		r = line_regex.search(line)
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
			if r.group(3) == "meetingsvc":
				continue
			out.append("\"service\":\"%s\"" % r.group(3))
			out.append("\"servlet\":\"%s\"" % r.group(4))
			message = r.group(5)
			if re.search("^END:", message):
				if re.search("^END:\ +{", message):
					while not re.search("\{.*\}$", message):
						next_line = regex.sub(b"", next(fin)).decode("utf-8").strip("\r").replace("\\r", "").replace("\\n", "").replace("\\u0000", "")
						next_line = wtf_regex.sub('"}', next_line)
						next_line = wtf2_regex.sub('","', next_line)
						message += next_line
				tmp = re.search("(\{.*\})", message).group(1)
				tmp = tmp.replace("true", "True")
				tmp = tmp.replace("false", "False")
				r = msg_regex.search(tmp)
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
					tar = "\"message\":\"%s\"" % msg.replace("\"", "\'") .replace("\\","")+ r.group(0)[-1]
					try:
						tmp = msg_regex.sub(tar, tmp)
					except sre_constants.error:
						errout.write(i, "sre_constants.error\n")
						continue
				r = path_regex.search(tmp)
				if r:
					path = r.group(1).replace("\"", "\'").replace("\\","")
					tar = "\"path\":\"%s\"" % path + r.group(0)[-1]
					tmp = path_regex.sub(tar, tmp)
				##
				# ignore addMembers, as the json is invalid
				# i.e., "addedMembers":[ac_ws-inttest-testGroup-1416945206192]
				#
				tmp = re.sub("\"(add|delet)edMembers\":\[.*?\],?", "", tmp)
				try:
					tags = eval(tmp)
				except SyntaxError:
					errout.write(i, "Syntax Error:", tmp, "\n")
					continue
				for x in tags:
					out.append("\"%s\":\"%s\"" % (x, tags[x]))
			else:
				##
				# ignore phase:START, since the info is duplicated in phase:END
				#
				if re.search("^START:", message):
					continue
				out.append("\"message\":\"%s\"" % message.replace("\"","\'").replace("\\",""))
			if csvOutput:
				#with gzip.open(log+".csv.gz","wt") as fout:
				with open(des+".csv","a") as fout:
					w = csv.DictWriter(fout, fieldnames = colName, delimiter = '|')
					w.writerow(eval("{" + ",".join(out) + "}\n"))
			if jsonOutput:
				with open(des+".json" ,"a") as fout:
					fout.write("{" + ",".join(out) + "}\n")
			# if redo_mode:
			# 	redo_mode_write = True		
				
# des = os.path.basename(log)

# if jsonOutput:
# 	with gzip.open(des+".json.gz" ,"wt") as fout:
# 		for line in output:
# 			fout.write(line)

# if csvOutput:
# 	#with gzip.open(log+".csv.gz","wt") as fout:
# 	with open(des+".csv","w") as fout:
# 		colName = ["timestamp", "service", "servlet", "user", "success", "method", "from", "message", "path", "time", "jobID", "bytes", "target"]
# 		w = csv.DictWriter(fout, fieldnames = colName, delimiter = '|')
# 		#w.writeheader()
# 		for line in output:
# 			w.writerow(eval(line))
