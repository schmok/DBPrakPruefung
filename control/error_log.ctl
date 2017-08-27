load data 
infile 'is-error_log.csv' "str '\n'"
append
into table PRUEFUNG.ERROR_LOG
fields terminated by ','
OPTIONALLY ENCLOSED BY '"' AND '"'
trailing nullcols
           (
             ERROR_TIME CHAR(4000),
             TYPE CHAR(4000),
             HOSTNAME CHAR(4000),
             CAUSE CHAR(4000),
             MESSAGE CHAR(4000),
             LOG_FILE CHAR(4000)
           )
