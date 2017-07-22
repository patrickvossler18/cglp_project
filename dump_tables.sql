SELECT * FROM case_info_test INTO OUTFILE '/tmp/case_info20170721.csv'
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'; 

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
FROM citations_test
INTO OUTFILE '/tmp/citations20170721.csv'
FIELDS TERMINATED BY ',' 
ESCAPED BY '"' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n';

# scp -i /Users/patrick/Dropbox/Fall\ 2016/SPEC/CGLP.pem ec2-user@54.202.27.95:/tmp/citations20170621.csv /Users/patrick/Dropbox/Fall\ 2016/SPEC/


# Compare counts from sequential method and parallel method
select f.country_name, f.first_count, s.second_count 
from (select ci.country_name, count(*) as `first_count` from case_info ct join country_ids ci on ci.country_id = ct.country_id group by ct.country_id) f
join (select ci.country_name, count(*) as `second_count` from case_info_test ct join country_ids ci on ci.country_id = ct.country_id group by ct.country_id) s
on f.country_name = s.country_name;