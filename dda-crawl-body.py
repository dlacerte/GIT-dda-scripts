#!/usr/bin/python

import psycopg2
import sys

# Connect to remote BB DDA Postgres instance using psycopg2 python module

# DDA postgres psql queries
all_content_items = "select pk1 from course_contents where crsmain_pk1 = %s "
all_parent_items = "select distinct(parent_pk1) from course_contents where parent_pk1 is not null and crsmain_pk1 = %s "
parent_of_item = "select parent_pk1 from course_contents where pk1 = %s and parent_pk1 is not NULL"
get_crs_name = "select course_id from course_main where pk1 = %s "
toc_query = "select course_contents_pk1 from course_toc where crsmain_pk1 = %s and target_type = 'CONTENT_LINK' " 
copy_from_query = "select copy_from_uuid from course_main where pk1 = %s and copy_from_uuid is not null "
uuid_query = "select copy_from_uuid from course_main where uuid = %s "
uuid_to_courseid_query = "select course_id from course_main where uuid = %s "
find_dependants_recursive_query = "with recursive sub as (select ancestor_pk1, descendant_pk1 from x_course_contents where ancestor_pk1 = %s union select x.ancestor_pk1, x.descendant_pk1 from x_course_contents x inner join sub s on s.descendant_pk1 = x.ancestor_pk1 ) select * from sub; "
find_ancestors_recursive_query = "with recursive sub as ( select descendant_pk1, ancestor_pk1 from x_course_contents where descendant_pk1 = %s union select x.ancestor_pk1, x.descendant_pk1 from x_course_contents x inner join sub s on s.ancestor_pk1 = x.descendant_pk1 ) select * from sub; "
get_content_ancestors = "select  ancestor_pk1, descendant_pk1, distance from x_course_contents where descendant_pk1 = %s "
get_content_type = "select pk1, content_type, cnthndlr_handle, text_format_type from course_contents where pk1 = %s "
get_cms_resource_link = "select parent_pk1, crsmain_pk1, parent_data_type, resource_id from cms_resource_link where crsmain_pk1 = %s " 
#get_content_type = "select * from course_contents where pk1 = %s "

# Provide course pk1 on command line
#crspk1 = sys.argv[1]

# DDA credentials
# SAAS LIVE = BB5a333e152baa2
db = psycopg2.connect(database='xxx', user='xxx', password='xxx', host='xxxproxy.edu', port='54320')

cur = db.cursor()

#FUNC: get_cms_res(crspk1)
def get_cms_res(crspk1):
	cur.execute( get_cms_resource_link, (crspk1,))
        rows = cur.fetchall()
	if rows != None:
		for row in rows:
			print row

#FUNC: uuid_to_courseid()
def uuid_to_courseid(uuidvalue):
	cur.execute( uuid_to_courseid_query, (uuidvalue, ))
        rows = cur.fetchone() 
	if rows != None:
		cuid = rows[0]
                return cuid
	
#FUNC: uuid_chain() returns a hiercy list of copied-from-uuis listing
def uuid_chain(uuidvalue):
        tab = '\t'
	cur.execute( uuid_query, (uuidvalue, ))
        rows = cur.fetchone() 
	if rows != None:
		cuid = uuid_to_courseid(rows[0])
		print tab + str(rows[0]) + ' ' + str(cuid) 
                tab = tab + '\t'
		uuid_chain(rows[0]) 

# FUNC: copy_from_list()
def copy_from(crspk1):
	cur.execute( copy_from_query, (crspk1,) )
        rows = cur.fetchone()
	if rows != None :
                cuid = uuid_to_courseid(rows[0])
		print "copied from ", str(rows[0]) + ' ' + cuid
                uuid_chain(rows[0])
        else: 
		print "not copied from any other UUID"


# FUNC: get_course_id (short name) 
def get_course_id(crspk1):
	cur.execute( get_crs_name, (crspk1,) )
        rows = cur.fetchone()
        #print rows
	name = rows[0]
        return str(name)	
      
# Function get ALL content for a given course_pk1
def all_contents(crspk1):
	global ALL_ITEMS
	cur.execute( all_content_items, (crspk1,) )
	rows = cur.fetchall()
	print "# items = ", len(rows)
        rng = range(len(rows))
	for line in rng: 
                t = rows[line]
                item_pk1 = int(t[0])
		#print "c=", item_pk1
                if item_pk1 not in ALL_ITEMS:
                      	ALL_ITEMS.append(item_pk1)

# FUNC: parent_content() get a disticnt list of parent content items for a given course pk1
def all_parents(crspk1):
        global PARENT_ITEMS
	cur.execute( all_parent_items, (crspk1,) )
	rows = cur.fetchall()
	print "# parent items = ", len(rows)
        rng = range(len(rows))
	for line in rng: 
                t = rows[line]
                #print "p=", int(t[0])
		PARENT_ITEMS.append(int(t[0]))
                
#FUNC: recursive_parents() returns the parent
def recurse_parents(pk1_list):
        global PLIST
        global REC_COUNT 
        REC_COUNT+=1
        if REC_COUNT >= 10: 
		sys.exit() 
        plist = []
        for item in pk1_list:
		cur.execute( parent_of_item, ( item, ))
		row = cur.fetchall()
                #print "row =", row 
        	if len(row) > 0 :
                        #print "LEN = ", len(row)
			t = row[0]
                        parent_pk1 = int(t[0])
			#print "item = ", parent_pk1
                        if parent_pk1 not in plist:
                        	plist.append(parent_pk1)
        print "RECURSE count = ", REC_COUNT
        if len(plist) > 0:
        	print "PLIST = ", plist
		plist2 = plist 
                recurse_parents(plist2)
        return plist

# FUNC: get_toc()
def get_toc(crspk1):
	global TOC_LIST 
	cur.execute( toc_query, (crspk1,) )
        rows = cur.fetchall()
	print "# ToC items = ", len(rows)
        #print "row =", rows
        rng = range(len(rows))
        if len(rows) > 0 :
		for item in rng:
			t = rows[item]
                	#print "t= ", t
			TOC_ITEMS.append(int(t[0]))

# FUNC: get_ancestors(content_pk1)
def get_ancestors(content_pk1):
	cur.execute( get_content_ancestors, (content_pk1,) )
        rows = cur.fetchall()
	#print "# ToC items = ", len(rows)
        #print "row =", rows
        rng = range(len(rows))
        if len(rows) > 0 :
		for item in rng:
			t = rows[item]
                	print "anc/des/dis= ", t
# FUNC: get_handle(content_pk1)
def get_handle(content_pk1):
	cur.execute( get_content_type, (content_pk1,) )
        rows = cur.fetchall()
	#print "# ToC items = ", len(rows)
        #print "row =", rows
        rng = range(len(rows))
        if len(rows) > 0 :
		for item in rng:
			t = rows[item]
                	print "cnt = ", t

        
# MAIN: Process command line arguments (crsmain_pk1) courseids
numargs = len(sys.argv)
#print "num args = ", numargs
for num in range(1, numargs):
	crspk1 = sys.argv[num]

        if str.isdigit(crspk1):
		print "IS digit"
	else:
		print "IS NOT digits"
##        sys.exit()

	ALL_ITEMS = []
        # parent_items = folders
	PARENT_ITEMS = []
        # toc_items = table of contents menu (top folder) items
	TOC_ITEMS = []
	REC_COUNT = 0

        cname = get_course_id(crspk1) 
        print '\n'
        print "COURSE ID = ", cname
        print "COURSE PK1 = ", crspk1 


	all_contents(crspk1)
	print "ALL CONTENT = ", ALL_ITEMS

	all_parents(crspk1)
	print "PARENT CONTENT = ", PARENT_ITEMS

        get_toc(crspk1) 
        print "TOC LIST = ", TOC_ITEMS

        get_cms_res(crspk1)

	copy_from(crspk1)

        # Subtract parent_items (folders) from the ALL_ITEMS list of content items
        BASE_LIST = [x for x in ALL_ITEMS if x not in PARENT_ITEMS]
        NEW_BASE_LIST = [x for x in BASE_LIST if x not in TOC_ITEMS ]
        print "length of base list = ", len(NEW_BASE_LIST)

        for tocitem in TOC_ITEMS:
		if tocitem not in ALL_ITEMS:
			print "toc item is NOT in content ", tocitem


##for citem in NEW_BASE_LIST:
	#print "item = ", citem 
######	get_handle( citem )
#        get_ancestors( citem )
        

print '\n'
