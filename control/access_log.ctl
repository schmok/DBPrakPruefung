load data 
infile 'access_log.csv' "str '\n'"
append
into table PRUEFUNG.ACCESS_LOG
fields terminated by ','
OPTIONALLY ENCLOSED BY '"' AND '"'
trailing nullcols
           (
             HOSTNAME CHAR(4000),
             REMOTE_LOGNAME CHAR(4000),
             USER_ID CHAR(4000),
             REQUEST_TIME CHAR(4000),
             METHOD CHAR(4000),
             URL CHAR(4000),
             PROTOCOL CHAR(4000),
             STATUS_CODE CHAR(4000),
             RESPONSE_BODY_SIZE CHAR(4000),
             REFERRER CHAR(4000),
             USER_AGENT CHAR(4000),
             SERVER_ID CHAR(4000)
           )
