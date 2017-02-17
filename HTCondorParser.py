import sys
import csv
import re

if "-pre" in sys.argv:
	preOS = True
elif "-post" in sys.argv:
	preOS = False
else:
	print("missing OpenStack flag (-pre/-post) ...")
	sys.exit(0)

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
		
# list of fields that need no manipulation
implicit = [
"JobStatus", 
"CommittedTime", # duration of the job including suspension
"CommittedSuspensionTime", # suspension time of the job
"CumulativeSlotTime", # core seconds
"NumJobStarts",
"RequestCpus",
"JobStartDate",
"VMName", # only available preOS
"RemoveReason",
"CompletionDate"
]
explicit = ["QDate", "Project", "Owner", "VMInstanceType", "VMInstanceName", "VMSpec.CPU", "VMSpec.RAM", "VMSpec.DISK", "RequestMemory", "RequestDisk", "MemoryUsage", "DiskUsage"]

# the table mapping VM uuid to specifications
VM = {
#	VM_ID								RAM(MB)		DISK(GB)	SCRATCH(GB)		CPU
"083093b3-ffc1-464e-a453-cefce795021b":[ 6144		, 0			, 0				, 4		],
"0eb207f9-4575-4bd2-a430-1ed50e821d05":[ 61440		, 20		, 186			, 8		],
"2cb70964-721d-47ff-badb-b702898b6fc2":[ 12288		, 0			, 0				, 8		],
"2e33b8b5-d8d1-4fd8-913c-990f34a89002":[ 7680		, 20		, 31			, 2		],
"2ff7463c-dda9-4687-8b7a-80ad3303fd41":[ 3072		, 0			, 0				, 2		],
"327fa6c5-4ec8-432d-9607-cd7f40252320":[ 92160		, 20		, 186			, 8		],
"39e8550a-c2cf-4094-93c0-fb70b35b6a6c":[ 1536		, 0			, 0				, 1		],
"4998f4d2-b664-4d9d-8e0d-2071f3e44b10":[ 30720		, 20		, 83			, 4		],
"4f61f147-353c-4a24-892b-f95a1a523ef6":[ 7680		, 20		, 30			, 1		],
"5c86b033-a1d0-4b6a-b1c9-4a57ad84d594":[ 30720		, 20		, 380			, 8		],
"6164f230-4859-4bf5-8f5b-fc450d8a8fb0":[ 15360		, 20		, 80			, 2		],
"64a90d5f-71fc-4644-bc64-f8d907249e35":[ 61440		, 20		, 780			, 16	],
"69174301-2d70-4bc1-9061-66b2eaff5d07":[ 15360		, 20		, 180			, 4		],
"7c7fdfc0-57e6-49e9-bbde-37add33e1681":[ 61440		, 20		, 380			, 8		],
"88e57477-b6a5-412e-85e0-69ff48ceb45c":[ 46080		, 20		, 180			, 4		],
"91407374-25de-4c0a-bd76-c0bdaecf47eb":[ 122880		, 20		, 392			, 16	],
"9493fdd3-3100-440d-a9a1-020d93701ed2":[ 15360		, 20		, 83			, 4		],
"aa8ca469-e939-40ba-964d-28bfd1c61480":[ 15360		, 20		, 31			, 2		],
"ac5155b2-87c8-42ed-9b56-edd00b3880cc":[ 122880		, 20		, 780			, 16	],
"b64b981a-e832-47e9-903f-fb98cff0579b":[ 61440		, 20		, 392			, 16	],
"bcb3eb5a-8485-4520-b06c-cb5a58bb482f":[ 30720		, 0			, 0				, 8		],
"c94c95cc-6641-475b-b044-98b24a22dcaa":[ 7680		, 20		, 80			, 2		],
"d2d56ca5-511b-4a4b-89eb-1d6f06ee58b1":[ 30720		, 20		, 186			, 8		],
"da751037-da00-4eff-bca1-0d21dafaa347":[ 46080		, 20		, 83			, 4		],
"de70f75f-83a0-43ce-8ac6-be3837359a0a":[ 30720		, 20		, 180			, 4		],
"df94e28a-8983-4b4a-baa8-ffb824591c23":[ 92160		, 20		, 380			, 8		],
# more
"13efd2a1-2fd8-48c4-822f-ce9bdc0e0004":[122880		, 20		, 780			, 16	],
"23090fc1-bdf7-433e-9804-a7ec3d11de08":[15360		, 20		, 80			, 2		],
"5112ed51-d263-4cc7-8b0f-7ef4782f783c":[46080		, 20		, 180			, 4		],
"6c1ed3eb-6341-470e-92b7-5142014e7c5e":[7680		, 20		, 80			, 2		],
"72009191-d893-4a07-871c-7f6e50b4e110":[61440		, 20		, 380			, 8		],
"8061864c-722b-4f79-83af-91c3a835bd48":[15360		, 20		, 180			, 4		],
"848b71a2-ae6b-4fcf-bba4-b7b0fccff5cf":[6144		, 0			, 0				, 8		],
"8953676d-def7-4290-b239-4a14311fbb69":[30720		, 20		, 380			, 8		],
"a55036b9-f40c-4781-a293-789647c063d7":[92160		, 20		, 380			, 8		],
"d816ae8b-ab7d-403d-ae5f-f457b775903d":[184320		, 20		, 780			, 16	],
"f9f6fbd7-a0af-4604-8911-041ea6cbbbe4":[768			, 0			, 0				, 1		],
# same table but aliases
"c16.med":[122880,20,780,16],
"c2.med":[15360,20,80,2],
"p8-12gb":[12288,0,0,8],
"c4.hi":[46080,20,180,4],
"c2.low":[7680,20,80,2],
"c8.med":[61440,20,380,8],
"c4.low":[15360,20,180,4],
"p8-6gb":[6144,0,0,8],
"c8.low":[30720,20,380,8],
"c8.hi":[92160,20,380,8],
"c16.hi":[184320,20,780,16],
"p1-0.75gb-tobedeleted":[768,0,0,1],
# more, from CC, scratch or disk space is missing so I put 0 as disk space
"126e8ef0-b816-43ed-bd5f-b1d4e16fdda0":[7680		, 0			, 80			, 2		],
"34ed4d1a-e7d5-4c74-8fdd-3db36c4bcbdb":[245760		, 0			, 1500			, 16	],
"4100db19-f4c9-4ac8-8fed-1fd4c0a282e5":[196608		, 0			, 1000			, 12	],
"5ea7bc52-ce75-4501-9978-fad52608809d":[122880		, 0			, 750			, 8		],
"8e3133f4-dc5a-4fdf-9858-39e099027253":[32768		, 0			, 0				, 16	],
"9431e310-432a-4edb-9604-d7d2d5aef0f7":[196608		, 0			, 1100			, 12	],
"9cabf7e3-1f74-463c-ab38-d46e7f92d616":[184320		, 0			, 780			, 16	],
"d67eccfe-042b-4f86-a2fc-92398ebc811b":[7680		, 0			, 30			, 1		]	
}

ownerProj = {
"jkavelaars":"canfar-UVic_KBOs",
"mtb55":"canfar-UVic_KBOs",
"sgwyn":"canfar-moproc",
"durand":"canfar-HST-RW",
"sfabbro":"canfar-ots",
"ptsws":"canfar-ots",
"chenc":"canfar-ots",
"fraserw":"canfar-UVic_KBOs",
"lff":"canfar-ngvs",
"pashartic":"canfar-ngvs",
"gwyn":"canfar-moproc",
"glass0":"canfar-UVic_KBOs",
"patcote":"canfar-ngvs",
"lauren":"canfar-ngvs",
"jjk":"canfar-UVic_KBOs",
"yingyu":"canfar-ngvs",
"rpike":"canfar-UVic_KBOs",
"nickball":"canfar-cadc",
"woodleka":"canfar-pandas",
"cadctest":"canfar-cadc",
"markbooth":"canfar-debris",
"MarkBooth":"canfar-debris",
"goliaths":"canfar-cadc",
"dschade":"canfar-cadc",
"pritchetsupernovae":"canfar-ots",
"ryan":"CANFAROps",
"jroediger":"canfar-ngvs",
"cshankm":"canfar-UVic_KBOs",
"fabbros":"canfar-ots",
"jouellet":"canfar-cadc",
"jonathansick":"canfar-androphot",
"shaimaaali":"canfar-shaimaaali",
"ShaimaaAli":"canfar-shaimaaali",
"johnq":"canfar-ngvs",
"helenkirk":"canfar-jcmt",
"canfradm":"CANFAR",
"clare":"canfar-ots",
"taylorm":"canfar-UVic_KBOs",
"nhill":"canfar-cadc",
"canfrops":"CANFAROps",
"laurenm":"canfar-ngvs",
"nick":"canfar-cadc",
"layth":"canfar-ngvs",
"jpveran":"canfar-aot",
"jpv":"canfar-aot",
"caread966":"canfar-ngvs",
"nvulic":"canfar-nvulic",
"kwoodley":"canfar-pandas",
"sanaz":"canfar-cfhtlens",
"cadcauthtest1":"canfar-cadc",
"dcolombo":"canfar-mwsynthesis",
"fpierfed":"canfar-HST-RW",
"echapin":"canfar-scuba2",
"jenkinsd":"canfar-cadc",
"brendam":"canfar-debris",
"russell":"canfar-cadc",
"trystynb":"canfar-ngvs",
"davids":"canfar-cadc",
"scott":"canfar-scuba2",
"streeto":"canfar-streeto",
"matthews":"canfar-debris",
"jrseti":"canfar-seti",
"bsibthorpe":"canfar-debris",
"gerryharp":"canfar-seti",
"hguy":"canfar-canarie",
"majorb":"canfar-cadc",
"samlawler":"canfar-debris",
"cadcsw":"canfar-cadc",
"canfar":"CANFAR",
"markb":"canfar-debris"   
}
with open(log, "r", encoding='utf-8') as fin:
	# entire output file
	output = []
	# each line
	out = []
	# used to calc mem usage
	# -1 is to be handled by logstash
	residentSetSize = 0
	diskUsg = 0
	imgSize = 0
	stDate, endDate = 0, 0
	# used for early logs where VMInstanceType is not defined
	# where the spec of vm is written in three separate fields
	vmCPUCores, vmMem, vmStorage = 0, 0, 0
	# for preOS logs it is possible that RequestMemory is not available in Requirements
	# i.e.:		Requirements = (VMType =?= "nimbus_test" && Arch == "INTEL" && Memory >= 2048 && Cpus >= 1)
	#			==> RequestMemory = 2048
	#			-vs-
	#			Requirements = (Arch == "INTEL") && (OpSys == "LINUX") && (Disk >= DiskUsage) && (((Memory * 1024) >= ImageSize)
	#			==> RequestMemory unknown
	# then RequestMemory is assumed to be VMMem
	findReqMem = False
	# for preOS we might not be able to find the project name in VMLoc field
	# then proj name = owner
	#findProj = False
	# if it is year 2014. 2014 is different: even tho it is preOS but MemoryUsage is calculated by postOS method
	yr2014 = False
	# preOS, Project is traslate through ownerProj dictionary
	owner = ''
	# a set to dissolve timestamp conflicts
	ts = set()
	content = fin.readlines()
	for i, line in enumerate(content):
		t = line.strip().split(" = ", 1)
		# if the current line is not "*** offset ...."
		if not re.match("\*\*\*", t[0]) :
			if any( t[0] == x for x in implicit):
				pass
			# convert QDate into millisecond and add 1 ms to avoid collision
			elif t[0] == "QDate":
				t[1] = int(t[1]) * 1000
				k = 1
				tmp = t[1]
				while tmp in ts:
					tmp = t[1] + k
					k += 1
				ts.add(tmp)
				# in ms: 2014-1-1 00:00:00		2015-1-1 00:00:00
				if t[1] >= 1388534400000 and tmp < 1420070400000:
					yr2014 = True 
				t[1] = str(tmp)
			#elif t[0] == "RemoveReason":
			#	t[1] = '"' + t[1] + '"'
			elif t[0] == "LastRemoteHost":
				t[0] = "VMInstanceName"
			elif t[0] == "Owner":
				owner = t[1][1:-1]
			elif t[0] == "VMCPUCores":
				vmCPUCores = int(t[1].replace('"', ''))
				continue
			elif t[0] == "VMMem":
				if re.search("G", t[1]):
					vmMem = int(t[1].replace('"', '').replace("G","")) * 1024
				else:
					vmMem = int(t[1].replace('"', '').replace("G",""))
				continue
			elif t[0] == "VMStorage":
				vmStorage = int(t[1].replace('"', '').replace("G",""))
				continue
			# from Requirements grab:
			# RequestMemory, if available (preOS)
			# Project:VMName (postOS)
			elif t[0] == "Requirements":
				if preOS:
					try:
						out.append('"RequestMemory":%s' % re.search("Memory\ \>\=\ (\d+)\ \&{2}\ Cpus", t[1]).group(1))
						findReqMem = True
					except AttributeError:
						pass
				else:
					r = re.search("VMType\ \=\?\=\ \"(.+)\:([^\"]+)?\"", t[1])
					# there are cases in history.20160304T220132 history.20150627T031409 history.20150704T171253, that we can't find Proj:VMNam in Requirements, and I will not catch this exception. 
					if r:
						out.append( '"Project":"%s"' % r.group(1) )
						out.append( '"VMName":"%s"' % r.group(2) )
					else:
						print("Can't find Proj:VMNam at line %i" % i)
				continue
			# grab Project from VMLoc, preOS
			#elif t[0] == "VMLoc" and preOS:
			#	try:
			#		t[1] = '"' + re.search("vospace\/([^\/]+)\/", t[1]).group(1) + '"'
			#		t[0] = "Project"
			#		findProj = True
			#	except AttributeError:
			#		continue
			# from "VMInstanceType" grab the vm flavor, and translate into VMSpecs
			elif t[0] == "VMInstanceType" and not preOS:
				spcKey = ""
				try:
					spcKey = re.search('\:(.*)\"', t[1]).group(1)
					if spcKey == "5c1ed3eb-6341-470e-92b7-5142014e7c5e" or spcKey == "12345678-6341-470e-92b7-5142014e7c5e":
						pass
					out.append('"VMSpec.RAM":%i' % VM[spcKey][0])
					out.append('"VMSpec.DISK":%i' % (VM[spcKey][1] + VM[spcKey][2]))
					out.append('"VMSpec.CPU":%i' % VM[spcKey][3])
				except KeyError:
					out.append('"VMSpec.RAM":%i' % 0)
					out.append('"VMSpec.DISK":%i' % 0)
					out.append('"VMSpec.CPU":%i' % 0)
				t[1] = '"' + spcKey + '"'
			elif t[0] == "ResidentSetSize":
				residentSetSize = int(t[1])
				continue
			# convert to mb
			elif t[0] == "DiskUsage":
				diskUsg = int(t[1]) / 1000
				t[1] = str(diskUsg)
			# convert to mb
			elif t[0] == "ImageSize":
				imgSize = int(t[1])
				continue
			else:
				continue
			out.append( "\"" + t[0].strip() + "\":" + t[1] )
		else:
			if preOS:# and not yr2014:
				try:
					out.append('"Project":"%s"' % ownerProj[owner])
				except KeyError:
					out.append('"Project":""')
				out.append('"VMSpec.RAM":%i' % vmMem)
				out.append('"VMSpec.DISK":%i' % vmStorage)
				out.append('"VMSpec.CPU":%i' % vmCPUCores)
				if not yr2014:
					out.append('"MemoryUsage":%.3f' % (imgSize / 1000))
					if not findReqMem:
						out.append('"RequestMemory":%i' % vmMem)
					#if not findProj:
					#	out.append('"Project":%s' % owner)
				else:
					# for 2014, MemoryUsage = ( residentSetSize + 1023 ) / 1024
					memoUsg = 0 if residentSetSize == 0 else (( residentSetSize + 1023 ) / 1024)
					out.append('"MemoryUsage":%.3f' % memoUsg)
					# for 2014, RequestMemory = MemoryUsage if MemoryUsage!=Null else ( ImageSize + 1023 ) / 1024
					out.append('"RequestMemory":%.3f' % ((0 if imgSize == 0 else (imgSize + 1023) / 1024) if memoUsg == 0 else memoUsg))
			# yr 2014 and postOS, fairly straightforward
			else:
				# for postOS, MemoryUsage = ( residentSetSize + 1023 ) / 1024
				memoUsg = 0 if residentSetSize == 0 else (( residentSetSize + 1023 ) / 1024)
				out.append('"MemoryUsage":%.3f' % memoUsg)	
				# for postOS, RequestMemory = MemoryUsage if MemoryUsage!=Null else ( ImageSize + 1023 ) / 1024
				out.append('"RequestMemory":%.3f' % ((0 if imgSize == 0 else (imgSize + 1023) / 1024) if memoUsg == 0 else memoUsg))
			# for all years, RequestDisk = DiskUsage
			out.append('"RequestDisk":%.3f' % diskUsg)
			# if the job finishes, compute the duration
			# merge to ES JSON
			# change if want to merge into other format
			output.append("{" + ",".join(out) + "}\n")
			# reset all the vars
			rmRsn, owner = [""] * 2
			out = []
			#[findReqMem, findProj, yr2014, findRmRsn] = [False] * 4
			[findReqMem, yr2014, findRmRsn] = [False] * 3
			[residentSetSize, diskUsg, imgSize, vmCPUCores, vmMem, vmStorage, stDate, endDate] = [0] * 8

if jsonOutput:
	with open(log+".JSON","w") as fout:
		for out in output:
			fout.write('%s' % out)

if csvOutput:
	with open(log+".csv","w") as fout:
		colName = explicit + implicit
		w = csv.DictWriter(fout, fieldnames = colName)
		w.writeheader()
		for line in output:
			w.writerow(eval(line))
