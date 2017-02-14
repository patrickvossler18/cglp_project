DROP TABLE IF EXISTS citations;
DROP TABLE IF EXISTS case_info;
CREATE TABLE `citations`(
`id` int(11) NOT NULL,
`citation_id` int(11) NOT NULL AUTO_INCREMENT,
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
PRIMARY KEY (`id`,`citation_id`)
)ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `case_info`(
`id` int(11) NOT NULL,
`case_id` varchar(100) DEFAULT NULL,
`decision_date` varchar(100) DEFAULT NULL,
`participant_name` varchar(2000) DEFAULT NULL,
`country_id` int(11) DEFAULT NULL,
`year` int (11) DEFAULT NULL,
`source_file_name` varchar(100) DEFAULT NULL,
PRIMARY KEY (`id`)
)ENGINE=MyISAM DEFAULT CHARSET=utf8;