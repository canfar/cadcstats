# cadcstats

1. condor
  * ```condor_parser.py```: Reads directly from htcondor history file and parsing useful fields into line-based JSON.
  * ```condor-es.conf```: The configuration file for Logstash.

2. tomcat 
  * ```tomcat_parser.py```: Reads from proc gz logs and parse useful lines into "contracted" JSON / csv format.
  * ```tomcat-es.conf```: The corresponding config for Logstash.

3. openstack
  * ```openstack-es.conf```: The config for Logstash.

4. advancedsearch
  * ```ip2dom.json```: An ip-to-domain dictionary.
  * ```adv2csv.py```: Dump and transpose ```cvodb1``` and ```cvodb2-01``` advanced search query history to a csv file.
  * ```advancedsearch-es.conf```: Logstash config file.

5. apache:
  * ```apache.conf```: The config for Logstash.

6. svc_plots:
  * A python package with all the subroutines to generate various plots. The proper name wrapper of each subroutine is within ```svc_plots.plots```. Please see the Jupyter notebook for each services for details.
  *  ```tomcat_odin.py``` is not included in ```svc_plots.plots```. These plotting subroutines can only use ```odin.cadc.dao.nrc.ca``` instead of ```elk.canfar.net```.

7. ```*.ipynbs```:
  * Jupyter notebooks that used to host all the services plots.
