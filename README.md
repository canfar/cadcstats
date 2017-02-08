# cadcstats
Tools/scripts used to collect history data from CADC services.

* ```HTCondorParser.py```: it reads directly from htcondor history file and parsing useful fields into "contracted" JSON / csv format that will be piped into Logstash.
* ```condor\_partial\_mappings.sh```: uploads the Elasticsearch mapping.
* ```condor-partial-es.conf```: the corresponding configuration file for Logstash.
* ```HTCondorToCsv.sh```: saves desired fields in a csv format, with all fields unevaluated.
