SELECT * FROM case_info INTO OUTFILE '/tmp/case_info20170901.csv'
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'; 

SELECT id,
replace(replace(replace(case_id, '\r\n', ' '), '\r', ' '),'""', '"'),
replace(replace(replace(decision_date, '\r\n', ' '), '\r', ' '),'""', '"'),
replace(replace(replace(participant_name, '\r\n', ' '), '\r', ' '),'""', '"'),
country_id,
year,
source_file_name
FROM case_info
INTO OUTFILE '/tmp/case_info20170911_fix.csv'
FIELDS TERMINATED BY ',' 
ESCAPED BY '"' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n';


SELECT id,
ifnull(citation_id, ""),
ifnull(citation_type, ""),
ifnull(year, ""),
ifnull(source_country_id, ""),
ifnull(source_court_id, ""),
ifnull(country_id, ""),
ifnull(court_code, ""),
ifnull(court_id, ""),
ifnull(intl_crt_id, ""),
ifnull(treaty_id, ""),
ifnull(softlaw_id, ""),
replace(replace(context,'\r\n',' '),'\r',' '),
source_file_name
FROM citations
INTO OUTFILE '/tmp/citations20170901.csv'
FIELDS TERMINATED BY ',' 
ESCAPED BY '"' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n';

# scp -i /Users/patrick/Dropbox/Fall\ 2016/SPEC/CGLP.pem ec2-user@54.202.27.95:/tmp/citations20170621.csv /Users/patrick/Dropbox/Fall\ 2016/SPEC/


# Compare counts from sequential method and parallel method
select f.country_name, f.first_count, s.second_count 
from (select ci.country_name, count(*) as `first_count` from case_info ct join country_ids ci on ci.country_id = ct.country_id group by ct.country_id) f
join (select ci.country_name, count(*) from case_info_test ct join country_ids ci on ci.country_id = ct.country_id group by ct.country_id) s
on f.country_name = s.country_name;


select ci.country_name, count(*) from citations_test ct join country_ids ci on ci.country_id = ct.country_id group by ct.country_id

select i.country_name as `source country`, ci.country_name as `cited country`, i.cnt from (select ci.country_name,ct.country_id, count(*) as cnt from citations_test ct join country_ids ci on ci.country_id = ct.source_country_id group by ct.country_id) i join country_ids ci on ci.country_id = i.country_id order by `source country`;


select ci.country_name,ct.country_id, ct.context from citations_test ct join country_ids ci on ci.country_id = ct.source_country_id where ct.source_country_id = 11  limit 1;

