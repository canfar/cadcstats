
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import warnings

# $ echo 'cvodb2-01:5432:cvodb:cadcuws:bc5e755f2be8e8' >> ~/.pgpass && chmod 600 ~/.pgpass
# $ echo 'cvodb1:5432:cvodb:cadcuws:bc5e755f2be8e8' >> ~/.pgpass
engine = create_engine('postgresql://cadcuws@cvodb2-01:5432/cvodb')
engine_old = create_engine('postgresql://cadcuws@cvodb1:5432/cvodb')


# In[2]:

query_test = "SELECT job.jobid, job.runid, job.ownerid, job.executionphase as phase, job.executionduration as duration, job.starttime, job.remoteip, jd.value as query FROM uws.Job job INNER JOIN uws.JobDetail jd ON job.jobid = jd.jobid WHERE (jd.value LIKE 'SELECT Observation.observationURI AS \"Preview\"%') "
df_test_new = pd.read_sql_query(text(query_test), con = engine)
df_test_old = pd.read_sql_query(text(query_test), con = engine_old)
df_test = pd.concat([df_test_old,df_test_new])


# In[3]:

warnings.simplefilter(action = "ignore", category = FutureWarning)
# checked
df_test["observation_id"] = df_test["query"].str.extract("lower\(Observation\.observationID\) (?:=|LIKE) '([^']+?)'")
# checked
df_test["pi_name"] = df_test["query"].str.extract("lower\(Observation\.proposal_pi\) LIKE '%([^']+?)%'")
# checked
df_test["proposal_id"] = df_test["query"].str.extract("lower\(Observation\.proposal_id\) LIKE '%([^']+?)%'")
# checked
df_test["proposal_title"] = df_test["query"].str.extract("lower\(Observation\.proposal_title\) LIKE '%([^']+?)%'")
# checked
df_test["proposal_keyword"] = df_test["query"].str.extract("lower\(Observation\.proposal_keywords\) LIKE '%([^']+?)%'")
# checked
df_test["data_release_date_public"] = (df_test["query"].str.contains("Plane\.dataRelease <=") & ~df_test["query"].str.contains("Plane\.dataRelease >="))
# checked
df_test.loc[df_test.data_release_date_public == False, "data_release_date"] =     df_test[df_test.data_release_date_public == False]["query"].str.extract("Plane\.dataRelease >= '(\d{4}\-\d{2}\-\d{2})")
# checked
df_test["observation_intention"] = df_test["query"].str.extract("lower\(Observation\.intent\) = '(calibration|science)'")
df_test.loc[df_test.observation_intention.isnull(), "observation_intention"] = "both"
# checked
df_test.loc[df_test["query"].str.contains("INTERSECTS\( CIRCLE\('ICRS',"), "target"] =     df_test[df_test["query"].str.contains("INTERSECTS\( CIRCLE\('ICRS',")]["query"].str.extract("INTERSECTS\( CIRCLE\('ICRS',(.+)\), Plane\.position_bounds \) = 1")
df_test.loc[df_test["query"].str.contains("lower\(Observation\.target_name\) ="), "target"] =     df_test[df_test["query"].str.contains("lower\(Observation\.target_name\) =")]["query"].str.extract("lower\(Observation\.target_name\) (?:=|LIKE) '([^']+?)'")
# checked
df_test["target_upload"] = df_test["query"].str.contains("TAP_UPLOAD\.search_upload")  
# checked
df_test["pixel_scale_left"] = df_test["query"].str.extract('Plane\.position_sampleSize >=? (\d+\.\d+(?:[Ee][+-]\d+)?)')
df_test["pixel_scale_right"] = df_test["query"].str.extract('Plane\.position_sampleSize <=? (\d+\.\d+(?:[Ee][+-]\d+)?)')
# checked
df_test["observation_date_left"] = df_test["query"].str.extract("INTERSECTS\( INTERVAL\( (\d+\.\d+), \d+\.\d+ \), Plane.time_bounds \) = 1")
df_test["observation_date_right"] = df_test["query"].str.extract("INTERSECTS\( INTERVAL\( \d+\.\d+, (\d+\.\d+) \), Plane.time_bounds \) = 1")
# checked
df_test["integration_time"] = df_test["query"].str.extract("Plane\.time_exposure = (\d+\.\d+(?:[Ee][+-]\d+)?)")
# checked
df_test["time_span"] = df_test["query"].str.extract("Plane\.time_bounds_width = (\d+\.\d+(?:[Ee][+-]\d+)?)")
# checked
df_test["spactral_coverage_left"] = df_test["query"].str.extract("INTERSECTS\( INTERVAL\( (\d+\.\d+(?:[Ee][+-]\d+)?), \d+\.\d+(?:[Ee][+-]\d+)? \), Plane.energy_bounds \) = 1")
df_test["spactral_coverage_right"] = df_test["query"].str.extract("INTERSECTS\( INTERVAL\( \d+\.\d+(?:[Ee][+-]\d+)?, (\d+\.\d+(?:[Ee][+-]\d+)?) \), Plane.energy_bounds \) = 1")
# checked
df_test["spactral_sampling_left"] = df_test["query"].str.extract("Plane\.energy_sampleSize >=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
df_test["spactral_sampling_right"] = df_test["query"].str.extract("Plane\.energy_sampleSize <=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
# checked
df_test["resolving_power_left"] = df_test["query"].str.extract("Plane\.energy_resolvingPower >=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
df_test["resolving_power_right"] = df_test["query"].str.extract("Plane\.energy_resolvingPower <=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
# checked
df_test["bandpass_width_left"] = df_test["query"].str.extract("Plane\.energy_bounds_width >=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
df_test["bandpass_width_right"] = df_test["query"].str.extract("Plane\.energy_bounds_width <=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
# checked
df_test["rest_frame_energy_left"] = df_test["query"].str.extract("Plane\.energy_restwav >=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
df_test["rest_frame_energy_right"] = df_test["query"].str.extract("Plane\.energy_restwav <=? (\d+\.\d+(?:[Ee][+-]\d+)?)")
# below all checked
df_test["band"] = df_test["query"].str.extract("Plane\.energy_emBand = '([^'']+?)'")
df_test["collection"] = df_test["query"].str.extract("Observation\.collection = '([^'']+?)'")
df_test["instrument"] = df_test["query"].str.extract("Observation\.instrument_name = '([^'']+?)'")
df_test["filter"] = df_test["query"].str.extract("Plane\.energy_bandpassName = '([^'']+?)'")
df_test["calibration_level"] = df_test["query"].str.extract("Plane\.calibrationLevel = '([^'']+?)'")
df_test["data_type"] = df_test["query"].str.extract("Plane\.dataProductType = '([^'']+?)'")
df_test["observation_type"] = df_test["query"].str.extract("Observation\.type = '([^'']+?)'")

del df_test["query"]


# In[4]:

DType = {"jobid":np.str_,
      "runid":np.str_,
      "ownerid":np.str_,
      "phase":np.str_,
      "duration":np.float64,
      "starttime":np.str_,
      "remoteip":np.str_,
      "observation_id":np.str_,
      "pi_name":np.str_,
      "proposal_id":np.str_,
      "proposal_title":np.str_,
      "proposal_keyword":np.str_,
      "data_release_date_public":np.bool_,
      "data_release_date":np.str_,
      "observation_intention":np.str_,
      "target":np.str_,
      "target_upload":np.bool_,
      "pixel_scale_left":np.float64,
      "pixel_scale_right":np.float64,
      "observation_date_left":np.float64,
      "observation_date_right":np.float64,
      "integration_time":np.float64,
      "time_span":np.float64,
      "spactral_coverage_left":np.float64,
      "spactral_coverage_right":np.float64,
      "spactral_sampling_left":np.float64,
      "spactral_sampling_right":np.float64,
      "resolving_power_left":np.float64,
      "resolving_power_right":np.float64,
      "bandpass_width_left":np.float64,
      "bandpass_width_right":np.float64,
      "rest_frame_energy_left":np.float64,
      "rest_frame_energy_right":np.float64,
      "band":np.str_,
      "collection":np.str_,
      "instrument":np.str_,
      "filter":np.str_,
      "calibration_level":np.float64,
      "data_type":np.str_,
      "observation_type":np.str_}


# In[7]:

df_test = df_test.astype(dtype = DType)


# In[10]:

df_test.to_csv("uws.csv", sep = ";", header = False)

