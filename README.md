# cadcstats
Tools/scripts used to collect history data from CADC services.

1. HTCondor
  * ```HTCondorParser.py```: Reads directly from htcondor history file and parsing useful fields into "contracted" JSON / csv.
  * ```condor_partial_mappings.sh```: Uploads the Elasticsearch mapping.
  * ```condor-partial-es.conf```: The corresponding configuration file for Logstash.

2. Tomcat
  * ```TomcatParser.py```: Reads from tomcat gz logs and parse useful lines into "contracted" JSON / csv format.
  * ```tomcat-es.conf```: The corresponding config for Logstash.

3. OpenStack
  * ```openstack-es.conf```: The corresponding config for Logstash.

4. AdvancedSearch
  * ```uws.ipynb```: Jupyter notebook that does some statistics.
