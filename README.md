# cadcstats
Tools/scripts used to collect history data from CADC services.

1. HTCondor
  * ```HTCondorParser.py```: Reads directly from htcondor history file and parsing useful fields into "contracted" JSON / csv.
  * ```HTCondorToCsv.sh```: Saves specified fields in a csv file, with all fields unevaluated.
  * ```condor_partial_mappings.sh```: Uploads the Elasticsearch mapping.
  * ```condor-partial-es.conf```: The corresponding configuration file for Logstash.

2. Tomcat
  * ```tomcatParser.py```: Reads from tomcat gz logs and parse useful lines into "contracted" JSON / csv format
