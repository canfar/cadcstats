# cadcstats

1. condor
  * ```condor_parser.py```: Reads directly from htcondor history file and parsing useful fields into line-based JSON. Usage: ```$ python3 condor_parser.py -f $FILE_IN -pre/-post -json```. ```-pre/-post``` is used to distinguish pre and post OpenStack.
  * ```condor-es.conf```: The configuration file for Logstash.

2. tomcat 
  * ```tomcat_parser.py```: Reads from tomcat gzipped logs and parse useful lines into line-based JSON format. Usage: ```$ python3 tomcat_parser.py -f $FILE_IN -json```. ```$FILE_IN``` must be the ```gzip``` tomcat logs.
  * There are two outputs: ```$FILE_IN.json``` and ```$FILE_IN.err```. The former is the line-based JSON and the later is a debug file contains the line number that the parser fails to parse, for what reason it fails, and the original raw string to be analyzed. 
  * The debug function (```-redo``` flag) is __experimental__, as it relies on grepping info from the file name, with the path to the log files being hard coded in the script. 
  * ```tomcat-es.conf```: The corresponding config for Logstash.

3. openstack
  * ```openstack-es.conf```: The config for Logstash.

4. advancedsearch
  * ```ip2dom.json```: An cached ip-to-domain dictionary.
  * ```adv2csv.py```: Dump and transpose ```cvodb1``` and ```cvodb2-01``` advanced search query history to a csv file. It uses a read-only account with the user name and password commented at the beginning of the script.
  * ```advancedsearch-es.conf```: Logstash config file.

5. apache:
  * ```apache.conf```: The config for Logstash.

6. svc_plots:
  * A python package with all the subroutines to generate various plots. The proper name wrapper of each subroutine is within ```plots.py```. Please see the Jupyter notebook for each services for details.
  *  ```tomcat_odin.py``` is not included in ```svc_plots.plots```. These plotting subroutines can only use ```odin.cadc.dao.nrc.ca``` instead of ```elk.canfar.net```.
  *  Other Python subroutines are packed and should be called from ```svc_plots.plots```

7. ```*.ipynbs```:
  * Jupyter notebooks that used to host all the services plots.

8. __@seb__:
  * For the unique file downloaded stats, i don't have a plot for that; however, under svc_plots dir, there is ```tomcat_old.fig7()``` that is not used anywhere. ```fig7()``` will do a query month by month, for the past two years, print the # of size of unique files. Again month by month actually reduces the computing over the server, compared to do two years at one time.
  * The certificates are under my home dir. All important stuff are either under my home dir or ```/data/```. Processed ```HTCondor``` and ```Tomcat``` logs are uploaded to ```vos:cadcstats```.
  * I have included some common Elasticsearch API commands in a seperate text file, ```es_commands.txt```. I found those commands very handy. The console portal on Kibana is very helpful when you have some ideas to try.
