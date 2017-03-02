#!/bin/bash

# total number of query
# psql -U cadcuws -h cvodb2-01 cvodb -c\
# 	"SELECT count(1) AS TotalQuery_Count
# 		FROM uws.Job job INNER join uws.JobDetail jd ON job.jobid = jd.jobid
# 		WHERE job.requestpath = '/tap/sync' 
# 		AND jd.name = 'QUERY' 
# 		AND jd.value like '%Observation.observationID%' 
# 		;"

# psql -U cadcuws -h cvodb2-01 cvodb -c\
# 	"SELECT count(1) AS ObservationID_count
# 		FROM uws.Job job INNER join uws.JobDetail jd ON job.jobid = jd.jobid
# 		WHERE job.requestpath = '/tap/sync' 
# 		AND jd.name = 'QUERY' 
# 		AND jd.value like '%lower(Observation.observationID) = %' 
# 		;"

# psql -U cadcuws -h cvodb2-01 cvodb -c\
# 	"SELECT count(1) AS PIName_count
# 		FROM uws.Job job INNER join uws.JobDetail jd ON job.jobid = jd.jobid
# 		WHERE job.requestpath = '/tap/sync' 
# 		AND jd.name = 'QUERY' 
# 		AND jd.value like '%WHERE  ( lower(Observation.proposal_pi) LIKE %' 
# 		;"

psql -U cadcuws -h cvodb2-01 cvodb -c\
		"\copy (
		SELECT
		COUNT(1) AS Tot_q,
		COUNT(1) FILTER (WHERE jd.value LIKE '%lower(Observation.observationID) =%') AS Observation_ID,
		COUNT(1) FILTER (WHERE jd.value LIKE '%lower(Observation.proposal_pi) LIKE%') AS PI_name,
		COUNT(1) FILTER (WHERE jd.value LIKE '%lower(Observation.proposal_id) LIKE%') AS proposal_id,
		COUNT(1) FILTER (WHERE jd.value LIKE '%lower(Observation.proposal_title) LIKE%') AS proposal_title,
		COUNT(1) FILTER (WHERE jd.value LIKE '%lower(Observation.proposal_keywords) LIKE%') AS Propsal_keyword,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.dataRelease >=% AND Plane.dataRelease <=%') AS Data_Rlease_Date,
		COUNT(1) FILTER (WHERE jd.value ~ 'WHERE.+[Plane\.dataRelease]{1}') AS Data_Rlease_Date_public,	
		COUNT(1) FILTER (WHERE jd.value LIKE '%lower(Observation.intent) = ''calibration''%') AS Calibration_only,
		COUNT(1) FILTER (WHERE jd.value LIKE '%lower(Observation.intent) = ''science''%') AS Science_only,

		COUNT(1) FILTER (WHERE jd.value LIKE '%TAP_UPLOAD.search_upload as f on INTERSECTS(POINT(''ICRS'', f.ra, f.dec)%') AS target_upload,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.position_sampleSize >=%AND  Plane.position_sampleSize <=%') AS pixel_scale,

		COUNT(1) FILTER (WHERE jd.value LIKE '%INTERSECTS( INTERVAL(%), Plane.time_bounds ) = 1%') AS observation_date,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.time_exposure =%') AS Integration_time,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.time_bounds_width =%') AS time_span,
		

		COUNT(1) FILTER (WHERE jd.value LIKE '%INTERSECTS( INTERVAL(%), Plane.energy_bounds ) = 1%') AS Spactral_Coverage,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.energy_sampleSize >%AND  Plane.energy_sampleSize <%') AS Spactral_Sampling,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.energy_resolvingPower >%') AS Resolving_power,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.energy_bounds_width >%AND  Plane.energy_bounds_width <%') AS Bandpass_width,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.energy_restwav >%AND  Plane.energy_restwav <%') AS rest_frame_energy,

		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.energy_emBand =%') AS band,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Observation.collection =%') AS Collection,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Observation.instrument_name =%') AS Instrument,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.energy_bandpassName =%') AS Filter,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.calibrationLevel =%') AS Calibration_level,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Plane.dataProductType =%') AS Data_type,
		COUNT(1) FILTER (WHERE jd.value LIKE '%Observation.type =%') AS Observation_type

		FROM uws.Job job INNER join uws.JobDetail jd ON job.jobid = jd.jobid
		WHERE job.requestpath = '/tap/sync' 
		AND jd.name = 'QUERY' 
		
		) To '/home/wliu/Desktop/out.csv' WITH CSV HEADER"						