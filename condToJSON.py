import sys
import re

if len(sys.argv) <= 2:
	print("please provide condor history file and OpenStack (-pre/-post) ...")
	sys.exit(0)
else:
	log = sys.argv[2]
	if sys.argv[1] == "-pre":
		preOS = True
	elif sys.argv[1] == "-post":
		preOS = False
	else:
		print("incorrect OpenStack flag (-pre/-post)...")
		sys.exit(0)

# list of fields that need no manipulation
include = [
"JobStatus", 
"CommittedTime", # duration of the job including suspension
"CommittedSuspensionTime", # suspension time of the job
"CumulativeSlotTime", # core seconds
"NumJobStarts",
"RequestCpus",
"JobStartDate",
"CompletionDate",
"RemoveReason",
"VMName" # only available preOS
]
# the table mapping VM uuid to specifications
VM = {
#	VM_ID								RAM(MB)		DISK(GB)	SCRATCH(GB)		CPU
"083093b3-ffc1-464e-a453-cefce795021b":[ 6144		 , 0		, 0				, 4			],
"0eb207f9-4575-4bd2-a430-1ed50e821d05":[ 61440		 , 20		, 186			, 8			],
"2cb70964-721d-47ff-badb-b702898b6fc2":[ 12288		 , 0		, 0				, 8			],
"2e33b8b5-d8d1-4fd8-913c-990f34a89002":[ 7680		 , 20		, 31			, 2			],
"2ff7463c-dda9-4687-8b7a-80ad3303fd41":[ 3072		 , 0		, 0				, 2			],
"327fa6c5-4ec8-432d-9607-cd7f40252320":[ 92160		 , 20		, 186			, 8			],
"39e8550a-c2cf-4094-93c0-fb70b35b6a6c":[ 1536		 , 0		, 0				, 1			],
"4998f4d2-b664-4d9d-8e0d-2071f3e44b10":[ 30720		 , 20		, 83			, 4			],
"4f61f147-353c-4a24-892b-f95a1a523ef6":[ 7680		 , 20		, 30			, 1			],
"5c86b033-a1d0-4b6a-b1c9-4a57ad84d594":[ 30720		 , 20		, 380			, 8			],
"6164f230-4859-4bf5-8f5b-fc450d8a8fb0":[ 15360		 , 20		, 80			, 2			],
"64a90d5f-71fc-4644-bc64-f8d907249e35":[ 61440		 , 20		, 780			, 16		],
"69174301-2d70-4bc1-9061-66b2eaff5d07":[ 15360		 , 20		, 180			, 4			],
"7c7fdfc0-57e6-49e9-bbde-37add33e1681":[ 61440		 , 20		, 380			, 8			],
"88e57477-b6a5-412e-85e0-69ff48ceb45c":[ 46080		 , 20		, 180			, 4			],
"91407374-25de-4c0a-bd76-c0bdaecf47eb":[ 122880		 , 20		, 392			, 16		],
"9493fdd3-3100-440d-a9a1-020d93701ed2":[ 15360		 , 20		, 83			, 4			],
"aa8ca469-e939-40ba-964d-28bfd1c61480":[ 15360		 , 20		, 31			, 2			],
"ac5155b2-87c8-42ed-9b56-edd00b3880cc":[ 122880		 , 20		, 780			, 16		],
"b64b981a-e832-47e9-903f-fb98cff0579b":[ 61440		 , 20		, 392			, 16		],
"bcb3eb5a-8485-4520-b06c-cb5a58bb482f":[ 30720		 , 0		, 0				, 8			],
"c94c95cc-6641-475b-b044-98b24a22dcaa":[ 7680		 , 20		, 80			, 2			],
"d2d56ca5-511b-4a4b-89eb-1d6f06ee58b1":[ 30720		 , 20		, 186			, 8			],
"da751037-da00-4eff-bca1-0d21dafaa347":[ 46080		 , 20		, 83			, 4			],
"de70f75f-83a0-43ce-8ac6-be3837359a0a":[ 30720		 , 20		, 180			, 4			],
"df94e28a-8983-4b4a-baa8-ffb824591c23":[ 92160		 , 20		, 380			, 8			]
}

with open(log,"r") as fin:
	# entire output file
	output = []
	# each line
	out = []
	# used to calc mem usage
	# -1 is to be handled by logstash
	residentSetSize = -1
	diskUsg = -1
	imgSize = -1
	stDate, endDate = -1, -1
	# used for early logs where VMInstanceType is not defined
	# where the spec of vm is written in three separate fields
	vmCPUCores, vmMem, vmStorage = -1, -1, -1
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
	findProj = False
	# if it is year 2014. 2014 is different: even tho it is preOS but MemoryUsage is calculated by postOS method
	yr2014 = False
	# if proj name is not found in preOS, proj = owner
	owner = ''
	# a set to dissolve timestamp conflicts
	ts = set()
	content = fin.readlines()
	for i, line in enumerate(content):
		t = line.strip().split(" = ", 1)
		# if the current line is not "*** offset ...."
		if not re.match("\*\*\*", t[0]) :
			if any( t[0] == x for x in include):
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
			elif t[0] == "LastRemoteHost":
				t[0] = "VMInstanceName"
			elif t[0] == "Owner":
				owner = t[1]
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
					r = re.search("VMType\ \=\?\=\ \"(.*)\:([^\"]*)?\"", t[1])
					# there are cases in history.20160304T220132 history.20150627T031409 history.20150704T171253, that we can't find Proj:VMNam in Requirements, and I will not catch this exception. 
					if r:
						out.append( '"Project":"%s"' % r.group(1) )
						out.append( '"VMName":"%s"' % r.group(2) )
					else:
						print("Can't find Proj:VMNam at line %i" % i)
				continue
			# grab Project from VMLoc, preOS
			elif t[0] == "VMLoc" and preOS:
				try:
					t[1] = '"' + re.search("vospace\/([^\/]*)\/", t[1]).group(1) + '"'
					t[0] = "Project"
					findProj = True
				except AttributeError:
					continue
			# from "VMInstanceType" grab the vm flavor, and translate into VMSpecs
			elif t[0] == "VMInstanceType" and not preOS:
				spcKey = ""
				try:
					spcKey = re.search('\:(.*)\"', t[1]).group(1)
					out.append('"VMSpec":%s' % VM[spcKey])
				except KeyError:
					out.append('"VMSpec":%s' % [-1, -1, -1, -1])	
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
			if preOS and not yr2014:
				out.append('"VMSpec":%s' % [vmMem, vmStorage, 0, vmCPUCores])
				out.append('"MemoryUsage":%.3f' % (imgSize / 1000))
				if not findReqMem:
					out.append('"RequestMemory":%i' % vmMem)
				if not findProj:
					out.append('"Project":%s' % owner)
			# yr 2014 and postOS, fairly straightforward
			else:
				# for postOS, MemoryUsage = ( residentSetSize + 1023 ) / 1024
				memoUsg = -1 if residentSetSize < 0 else (( residentSetSize + 1023 ) / 1024)
				out.append('"MemoryUsage":%.3f' % memoUsg)	
				# for postOS, RequestMemory = MemoryUsage if MemoryUsage!=Null else ( ImageSize + 1023 ) / 1024
				out.append('"RequestMemory":%.3f' % ((-1 if imgSize < 0 else (imgSize + 1023) / 1024) if memoUsg < 0 else memoUsg))
			# for all years, RequestDisk = DiskUsage
			out.append('"RequestDisk":%.3f' % diskUsg)
			# if the job finishes, compute the duration
			# merge to ES JSON
			# change if want to merge into other format
			output.append("{" + ",".join(out) + "}\n")
			# reset all the vars
			owner = ""
			out = []
			findReqMem, findProj, yr2014 = False, False, False			
			residentSetSize, diskUsg, imgSize, vmCPUCores, vmMem, vmStorage, stDate, endDate = -1, -1, -1, -1, -1, -1, -1, -1

with open(log+".JSON","w") as fout:
	for out in output:
		fout.write('%s' % out)
