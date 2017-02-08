# cadcstats
Tools/scripts used to collect history data from CADC services.

* ```HTCondorParser.py```: Reads directly from htcondor history file and parsing useful fields into "contracted" JSON / csv format that will be piped into Logstash.
* ```condor_partial_mappings.sh```: Uploads the Elasticsearch mapping.
* ```condor-partial-es.conf```: The corresponding configuration file for Logstash.
* ```HTCondorToCsv.sh```: Saves specified fields in a csv file, with all fields unevaluated.
