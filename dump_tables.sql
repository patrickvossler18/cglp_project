SELECT * FROM citations INTO OUTFILE '/tmp/citations20170319.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'; 

SELECT * FROM case_info INTO OUTFILE '/tmp/case_info20170319.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'; 