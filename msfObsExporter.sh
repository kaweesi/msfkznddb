#!/bin/bash

DB_USER=$1;DB_PASS=$2;DB_NAME=$3;RANGE=$4;OUTPUTDIR=$5;
STARTFROM=0;
TOTALCOUNT=`mysql -u$DB_USER -p$DB_PASS $DB_NAME -s -N  -e "select count(*) from obs"`
echo "Total Observations: $TOTALCOUNT"

while [ $STARTFROM -le $TOTALCOUNT ]
do
PERCENTAGE=$(awk "BEGIN { pc=100*${STARTFROM}/${TOTALCOUNT}; i=int(pc); print (pc-i<0.5)?i:i+1 }")
NOW=`date +%Y%m%d%H%M%S%N`
echo "............................................"
echo "Paging results from: $STARTFROM to $((STARTFROM+RANGE))"
echo "Progress: $PERCENTAGE%"
echo
mysql -u$DB_USER -p$DB_PASS $DB_NAME<<EOFMYSQL
SELECT 'Obs ID', 'Family Name', 'Given Name', 'Birth Date', 'Gender', 'Indentifier', 'Encounter Date', 'Provider/Clinician', 'Health Centre/Location', 'Observation Date', 'Observation Question', 'Observation Value'
UNION ALL(
	SELECT DISTINCT o.obs_id, pn.family_name, pn.given_name, p.birthdate, p.gender, id.identifier, e.encounter_datetime, pr.name AS provider, l.name AS location, o.obs_datetime, q.name AS obs_question, coalesce(ca.name, o.value_text, o.value_numeric, o.value_boolean, o.value_modifier, o.value_complex) AS obs_answer
	FROM obs o
	INNER JOIN person_name pn ON pn.person_id = o.person_id
	INNER JOIN person p ON p.person_id = o.person_id
	INNER JOIN patient_identifier id ON id.patient_id = o.person_id
	INNER JOIN encounter e ON e.patient_id = o.person_id
	INNER JOIN encounter_provider ep ON ep.encounter_id = e.encounter_id
	INNER JOIN provider pr ON pr.provider_id = ep.provider_id
	INNER JOIN location l ON l.location_id = o.location_id
	INNER JOIN concept_name q ON q.concept_id = o.concept_id
	INNER JOIN concept_name ca ON ca.concept_id = o.value_coded
	ORDER BY e.encounter_datetime, o.obs_datetime
	LIMIT $STARTFROM, $RANGE
	INTO OUTFILE '$OUTPUTDIR/obs_$NOW.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
);
EOFMYSQL
echo "............................................"
((STARTFROM=STARTFROM+RANGE))
done

