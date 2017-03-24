import re
import csv
import sys
import gzip
import time

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
		line = line.decode("utf-8").replace("\x00", "").strip("\n")
		if re.search("-\ END", line):
			# append timestamp
			t = int(time.mktime(time.strptime(re.match("(\d{4}\-\d{2}-\d{2}\ \d{2}\:\d{2}\:\d{2})", line).group(0), "%Y-%m-%d %H:%M:%S"))) * 1000
			k = 1
			tmp = t
			while tmp in ts:
				tmp = t + k
				k += 1
			ts.add(tmp)
			t = tmp
			out.append("\"timestamp\":%i" % t)
			# append service type
			try:
				out.append("\"service\":\"%s\"" % re.search("\d{3}\ (\w+)\ \[", line).group(1))
			except AttributeError:
				out.append("\"service\":\"NoService\"")
			try:		
				out.append("\"servlet\":\"%s\"" % re.search("\] INFO  (\w+)  - END: \{", line).group(1))
			except AttributeError:
				out.append("\"servlet\":\"NoServlet\"")	
			while not re.search("(\{.*\})", line):
				nextline = content[i + j].decode("utf-8").replace("\x00", "")
				line += nextline.strip("\n")
				j += 1		
			tmp = re.search("(\{.*\})", line).group(1)
			if re.search("true", tmp):
				tmp = tmp.replace("true", "True")
			if re.search("false", tmp):
				tmp = tmp.replace("false", "False")
			r = re.search("\"message\"\:\"(.*)\"\}$", tmp)
			if r:
				#msg = ""
				# if there is more than 10 '.' together, we remove them
				t = re.search("\.{10,}", r.group(1))
				if t:
					print("** find lots of '.' in %s" % log)
					msg = re.sub("\.{10,}", " ", r.group(1))
				else:
					msg = r.group(1)	
				tar = "\"message\":\"%s\"}" % msg.replace("\"", "\'")
				tmp = re.sub("\"message\"\:\"(.*)\"\}$", tar, tmp)
			tags = eval(tmp)
			#try:
			#	tags.pop("path")
			#except KeyError:
			#	pass
			for x in tags:
				if type(tags[x]) is str or type(tags[x]) is bool:
					out.append("\"%s\":\"%s\"" % (x, tags[x]))
				else:
					#if tags[x] == True or tags[x] == False:
					#	tags[x] = str(tags[x]).lower()
					out.append("\"%s\":%s" % (x, str(tags[x])))
			output.append("{" + ",".join(out) + "}\n")

if jsonOutput:
	with open(log+".json" ,"w") as fout:
		for line in output:
			fout.write(line)

if csvOutput:
	with open(log+".csv","w") as fout:
		colName = ["timestamp", "service", "servlet", "user", "success", "method", "from", "message", "path", "time", "jobID"]
		w = csv.DictWriter(fout, fieldnames = colName)
		w.writeheader()
		for line in output:
			w.writerow(eval(line))
