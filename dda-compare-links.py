#!/usr/bin/python

# PROGRAM: bbauto-course-copy 
# Author: Dave Lacerte March 21 2018 
# Modifications: 
#
# Here are the SQLite3 tables schema that this program uses: 
#sqlite> .schema
#CREATE TABLE commands ( crs_target text unique, crs_copy_cmd text, crs_disable_cmd text, crs_enroll_cmd text, crs_name_cmd text, crs_toc_cmd text, foreign key (crs_target) references copies (crs_target));
#CREATE TABLE copies ( crs_target text unique, crs_source text, crs_copy_date datetime , crs_copy_completed integer);

import time
import sqlite3
import sys
import datetime
import os 

# Define how long to keep a copy before a new copy is generated, and the old one id to be deleted
# Define maximum number of copy processes to launch in a given 24 hour period
max_copies_per_day = 200
sleep_between_copies = 3
sleep_between_commands = 1

# open sqlite database and read all of the cpypending records
sqdbfile = '/home/d/l/dlacerte/BBSQLite/BBAutoCourseCopy.db'
sqdb = sqlite3.connect( sqdbfile )
sqdb.text_factory = str
# create sqlite cursor object 'c'
c = sqdb.cursor()

# obtain a list of all target_courses which require a new course copy from the copies table (those with a NULL crs_copy_date) 
# crs-copy_completed = 0  ( aka False, not yet copied ) 
#c.execute( "select crs_target from copies where crs_copy_completed is null ")
c.execute( "select crs_target from copies where crs_target like '%-T18' " )
#c.execute( "select crs_target from copies ")
needscpy = c.fetchall()

get_cmd = "/home/d/l/dlacerte/bbapi-course get "

# Loop through each pending course copy with a sleep up to a maximium number of copies ( max_copies_per_day )
# Use an API call ( /usr/local/wcc/bin/bbapi-course copy CRS ) to initiate the copy on the BB SaaS target
# obtain the copy command for the uuid from the commands table
COPY_COUNTER = 1
for item in needscpy:
        NOW = datetime.datetime.now()
	new_target = item[0]
        print new_target
        CMD = get_cmd + new_target
        process = os.popen(CMD)
        results = process.read()
        print "COURSE RESULT = ", results
        process.close()

        
sqdb.close()
