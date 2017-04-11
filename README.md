# cadcstats
Tools/scripts used to collect history data from CADC services.

1. HTCondor
  * ```condor_parser.py```: Reads directly from htcondor history file and parsing useful fields into "contracted" JSON / csv.
  * ```condor.ipynb```: Some analytics.
  * ```condor_mappings.sh```: The Elasticsearch mappings.
  * ```condor-es.conf```: The configuration file for Logstash.
  * ```condor.py```: Analytics plotting.

2. tomcat 
  * ```tomcat_parser.py```: Reads from proc gz logs and parse useful lines into "contracted" JSON / csv format.
  * ```tomcat-es.conf```: The corresponding config for Logstash.

3. OpenStack
  * ```openstack-es.conf```: The config for Logstash.

4. AdvancedSearch
  * ```uws.ipynb```: Jupyter notebook that does some analytics.
  * ```ip2dom.json```: An ip-to-domain dictionary.
  * ```adv2csv.py```: Parsing ```cvodb1``` and ```cvodb2-01``` advanced search query history to a csv file.
  * ```advancedsearch-es.conf```: Logstash config file.
  * ```advancedsearch_mappings.sh```: Elasticsearch mappings.
  * ```advancedsearch.py```: Analytics plotting.

5. Apache:
  * ```apache.conf```: The config for Logstash.
