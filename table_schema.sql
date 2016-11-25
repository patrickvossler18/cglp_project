CREATE TABLE `citations`(
`case_id` int(11) NOT NULL AUTO_INCREMENT,
`citation_type` int(11) DEFAULT NULL,
`year` int (11) DEFAULT NULL,
`source_country_id` int(11) DEFAULT NULL,
`source_court_id` int(11) DEFAULT NULL,
`country_id` int(11) DEFAULT NULL,
`court_code` int(11) DEFAULT NULL,
`court_id` int(11) DEFAULT NULL,
`intl_crt_id` int(11) DEFAULT NULL,
`treaty_id` int(11) DEFAULT NULL,
`softlaw_id` int(11) DEFAULT NULL,
`context` varchar(2000) DEFAULT NULL,
`source_file_name` varchar(100) DEFAULT NULL,
PRIMARY KEY (`case_id`)
)ENGINE=MyISAM DEFAULT CHARSET=utf8;