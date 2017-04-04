import re
import csv
import sys
import gzip
import time
from datetime import datetime
import os

colName = ["timestamp", "service", "servlet", "user", "success", "method", "from", "message", "path", "time", "jobID", "bytes", "target"]
# buff_size = 3
# csvOutput = False
# jsonOutput = False
# redo_mode = False

class tomcatLog:
	def __init__(self, buff_size, log = None, csvOutput = False, jsonOutput = False, redo_mode = False, redo_lines = None):
		self.buff_size = buff_size
		self.log = log
		self.csvOutput = csvOutput
		self.jsonOutput = jsonOutput
		self.redo_mode = redo_mode
		self.redo_lines = redo_lines	
	def dest(self):
		return os.path.basename(self.log)

def main():
	tom = init(10000)
	parse(tom)

def init(buff_size):
	#global csvOutput, jsonOutput, redo_mode
	tomcat_log = tomcatLog(buff_size = buff_size)
	if "-csv" in sys.argv:
		tomcat_log.csvOutput = True 
	if "-json" in sys.argv:
		tomcat_log.jsonOutput = True 

	if not (tomcat_log.csvOutput or tomcat_log.jsonOutput):
		print("at least specify one type of output, -csv or -json ...")
		sys.exit(0)

	if "-redo" in sys.argv:
		tomcat_log.redo_mode = True
		tomcat_log.redo_lines = []
		redo_file = sys.argv[sys.argv.index("-redo") + 1]
		path = redo_file.split("_")
		tomcat_log.log = "/data/tomcat/" + path[0] + "/archive/" + path[1] + "/" + ".".join(redo_file.split(".")[:-1])
		with open(redo_file, "r") as fin:
			# this list of line int will always be sorted, since the lines were sequentially read and written
			for line in fin:
				tomcat_log.redo_lines.append(int(line.split()[0]))
			# tomcat_log.redo_lines = [int(_) for _ in fin.read().strip().split()]

	if not tomcat_log.redo_mode:
		try:
		    tomcat_log.log = sys.argv[sys.argv.index("-f") + 1]
		    try:
		        f = open(tomcat_log.log, "r")
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

	return tomcat_log    

def parse(tom):
	regex = re.compile(b'[\x00-\x1f]')
	line_regex = re.compile("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?) ([\w ]*) \[.+?\] INFO  (\w*)  - (.+)")
	msg_regex = re.compile("\"message\"\:\"(.*?)\"}$")
	path_regex = re.compile("\"path\":\"(.*?)(\",(?=\")|\"}$)")
	ua_regex = re.compile("\"userAgent\":\"{2}(.+?)\"{2},")
	query_regex = re.compile("\"query\":\"(.*?)(\",(?=\")|\"}$)")
	wtf_regex = re.compile('","}$')
	wtf2_regex = re.compile('",","')
	# ts = set()

	j = 0
	with gzip.open(tom.log, "rb") as fin, open(tom.dest() + ".err", "w") as errout:
		buff = []
		for i, line in enumerate(fin):
			if tom.redo_mode:
				if j >= len(tom.redo_lines):
					break
				line_num = tom.redo_lines[j]
				if i != line_num:
					continue
				else:
					j += 1	
			out = []
			line = regex.sub(b"", line).decode("utf-8").strip().replace("\\r", "").replace("\\n", "").replace("\\u0000", "")
			line = wtf_regex.sub('"}', line)
			line = wtf2_regex.sub('","', line)
			##
			# drop RemoteEventLogger and LogEventsServlet according to John's config
			# drop meetingsvc becoz we are not not interested
			#
			if re.search("RemoteEventLogger", line) or re.search("LogEventsServlet", line) or re.search("meetingsvc", line):
				continue
			##
			# group(1): time
			# group(2): optional milisec
			# group(3): service
			# group(4): servlet
			# group(5): message after '-'
			#
			r = line_regex.search(line)
			if r:
				# d = datetime.strptime(r.group(1), "%Y-%m-%d %H:%M:%S.%f")
				# t = int(time.mktime(d.timetuple())) * 1000 + d.microsecond / 1000
				# k = 1
				# tmp = t
				# while tmp in ts:
				# 	tmp = t + k
				# 	k += 1
				# ts.add(tmp)
				# t = tmp
				out.append("\"timestamp\":\"%s\"" % r.group(1))
				out.append("\"service\":\"%s\"" % r.group(3))
				out.append("\"servlet\":\"%s\"" % r.group(4))
				message = r.group(5)
				if re.search("^END:", message):
					if re.search("^END:\ +{", message):
						while not re.search("\{.*\}$", message):
							try:
								next_line = regex.sub(b"", next(fin)).decode("utf-8").strip("\r").replace("\\r", "").replace("\\n", "").replace("\\u0000", "")
							except StopIteration:
								errout.write("{} StopIteration, next(fin) reaches EOF!!\n".format(i))	
								write_output(buff, tom.dest(), tom.csvOutput, tom.jsonOutput)
								return
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
							errout.write("{} sre_constants.error\n".format(i))
							continue
					r = path_regex.search(tmp)
					if r:
						path = r.group(1).replace("\"", "\'").replace("\\","")
						tar = "\"path\":\"%s\"" % path + r.group(0)[-1]
						try:
							tmp = path_regex.sub(tar, tmp)
						except sre_constants.error:
							errout.write("{} sre_constants.error\n".format(i))
							continue
					r = query_regex.search(tmp)
					if r:
						query = r.group(1).replace("\"", "\'").replace("\\","")
						tar = "\"query\":\"%s\"" % query + r.group(0)[-1]
						try:
							tmp = path_regex.sub(tar, tmp)
						except sre_constants.error:
							errout.write("{} sre_constants.error\n".format(i))
							continue		
					r = ua_regex.search(tmp)
					if r:
						ua = r.group(1)	
						tar = "\"userAgent\":\"" + ua + "\","
					try:
						tmp = ua_regex.sub(tar, tmp)
					except sre_constants.error:
						errout.write("{} sre_constants.error\n".format(i))
						continue	
					##
					# ignore addMembers field, as the json is invalid
					# i.e., "addedMembers":[ac_ws-inttest-testGroup-1416945206192]
					# duplicated info is kept in other fields anyway
					#
					tmp = re.sub("\"(add|delet)edMembers\":\[.*?\],?", "", tmp)
					try:
						tags = eval(tmp)
					except SyntaxError:
						errout.write("{} Syntax Error: {}\n".format(i, tmp))
						continue
					except TypeError:
						errout.write("{} Type Error: {}\n".format(i, tmp))
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
				buff.append("{" + ",".join(out) + "}\n")
				#print(i, len(buff))
				if len(buff) >= tom.buff_size:
					write_output(buff, tom.dest(), tom.csvOutput, tom.jsonOutput)
					buff = []
		else:
			write_output(buff, tom.dest(), tom.csvOutput, tom.jsonOutput)

def write_output(buff, des, csvOutput, jsonOutput):			
	if csvOutput:
		#with gzip.open(log+".csv.gz","wt") as fout:
		with open(des+".csv","a") as fout:
			w = csv.DictWriter(fout, fieldnames = colName, delimiter = '|')
			for line in buff:
				w.writerow(eval(line))
	if jsonOutput:
		with open(des+".json" ,"a") as fout:
			for line in buff:
				fout.write(line)

if __name__ == "__main__":
	main()						
