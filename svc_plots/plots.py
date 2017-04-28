from . import advancedsearch, apache, condor, openstack, tomcat_odin, tomcat_old
from bokeh.plotting import output_notebook, show
from elasticsearch import Elasticsearch
import requests

es = 'http://elastic:cadcstats@elk.canfar.net:9200'

class Init():
	def __init__(self, url = None, timeout = 120):
		self.timeout = timeout
		if not url:
			self.url = es
		else:
			self.url = url	
		if not requests.get(self.url):
			print("Connection incorrect!")
			exit(0)
	def connect(self):
		return Elasticsearch(self.url, timeout = self.timeout)

def display(func):
	def wrapper(*args):
		r = func(*args)
		show(r)
	return wrapper	

##
# Advanced Search
#
@display
def advancedsearch_usg_of_each_field_all_domain(conn, idx):
	return advancedsearch.fig1(conn, idx)

@display
def advancedsearch_usg_of_each_field_exclusive(conn, idx):
	return advancedsearch.fig1(conn, idx, exclude = True)

@display
def advancedsearch_num_q_per_mo(conn, idx):
	return advancedsearch.fig2(conn, idx)

@display
def advancedsearch_num_q_per_mo_exclusive(conn, idx):
	return advancedsearch.fig2(conn, idx, exclude = True)

@display
def advancedsearch_num_q_per_ip(conn, idx):
	return advancedsearch.fig3(conn, idx)

@display
def advancedsearch_num_q_per_ip_201601(conn, idx):
	return advancedsearch.fig4(conn, idx)

def advancedsearch_avg_q_per_day(conn, idx):
	return advancedsearch.fig5(conn, idx)

@display
def advancedsearch_q_sub_by_dom(conn, idx):
	return advancedsearch.fig6(conn, idx)	

@display
def advancedsearch_num_q_by_dom(conn, idx):
	return advancedsearch.fig7(conn, idx, "clientdomain")

@display
def advancedsearch_most_freq_collection_for_others(conn, idx):
	return advancedsearch.fig7(conn, idx, "collection")

##
# Apache
#
@display
def community_ssos(conn, idx):
	return apache.fig1(conn, "logs-apache", "ssos")

@display
def community_dss(conn, idx):
	return apache.fig1(conn, "logs-apache", "dss")

@display
def community_meeting(conn, idx):
	return apache.fig1(conn, "logs-apache", "meeting")

@display
def community_yes(conn, idx):
	return apache.fig1(conn, "logs-apache", "yes")	

@display
def community_advancedsearch(conn, idx):
	return apache.fig1(conn, "logs-apache", "adv")

@display
def community_vosui(conn, idx):
	return apache.fig1(conn, "logs-apache", "vosui")			

##
# HTCondor
#
@display
def condor_num_restart(conn, idx):
	return condor.fig1(conn, idx)

@display
def condor_jobdur_vs_machdur(conn, idx):
	return condor.fig2(conn, idx)

@display
def condor_median_jobdur_vs_machdur(conn, idx):
	return condor.fig3(conn, idx)

@display
def condor_hist_jobdur_over_machdur_ratio(conn, idx):
	return condor.fig4(conn, idx)

@display
def condor_unique_users(conn, idx):
	return condor.fig5(conn, idx)

@display
def condor_ram_disk_req_vs_vmspec(conn, idx):
	return condor.fig6(conn, idx)	

@display
def condor_jobs_disk_ram_usage_per_yr(conn, idx):
	return condor.fig7(conn, idx)

@display
def condor_median_ramusg_ramusg_over_reqram_ratio(conn, idx):
	return condor.fig8(conn, idx)															

@display
def condor_ramusg_reqram_per_proj(conn, idx):
	return condor.fig9(conn, idx)

@display
def condor_unique_user_websub_vs_batch(conn):
	return condor.fig10(conn)

@display
def condor_websub_post_vs_get(conn):
	return condor.fig11(conn)

@display
def condor_websub_duration_post_vs_get(conn):
	return condor.fig12(conn)

@display
def condor_websub_over_batch_ratio(conn):
	return condor.fig13(conn)	

##
# OpenStack
#
@display
def openstack_cpuusg_os_vs_condor_1516(conn):
	return openstack.fig1(conn)	

@display
def openstack_usg_per_proj(conn, idx):
	return openstack.fig2(conn, idx)

@display
def openstack_cpuusg_os_vs_condor(conn):
	return openstack.fig3(conn)	

@display
def openstack_cpuusg_numvms(conn, idx):
	return openstack.fig4(conn, idx)	

##
# Tomcat data transfer
#
@display
def tomcat_datatransfer_timeevo(conn, idx):
	return tomcat_old.fig2(conn, idx)

@display
def tomcat_transfer_rate_batch_dot_canfar_dot_net_vs_num_batch_jobs(conn, idx):
	return tomcat_old.fig3(conn, idx)	

@display
def tomcat_datatransfer_num_n_size(conn, idx):
	return tomcat_old.fig4(conn, idx)

@display
def tomcat_data_vos_num_events(conn, idx):
	return tomcat_old.fig5(conn, idx)

@display
def tomcat_dl_rate_past_tw_yrs(conn, idx):
	return tomcat_old.fig6(conn, idx)	

@display
def tomcat_f_rate(conn, idx):
	return tomcat_old.fig8(conn, idx)														