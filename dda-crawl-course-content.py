#!/usr/bin/python

import psycopg2
import sys
import tempfile
import re

# Connect to remote BB DDA Postgres instance using psycopg2 python module

# DDA postgres psql queries
all_content_items = "select pk1 from course_contents where crsmain_pk1 = %s "
all_parent_items = "select distinct(parent_pk1) from course_contents where parent_pk1 is not null and crsmain_pk1 = %s "
parent_of_item = "select parent_pk1 from course_contents where pk1 = %s and parent_pk1 is not NULL"
get_crs_name = "select course_id from course_main where pk1 = %s "
get_crs_id = "select pk1 from course_main where course_id = %s "
toc_query = "select course_contents_pk1 from course_toc where crsmain_pk1 = %s and target_type = 'CONTENT_LINK' " 
copy_from_query = "select copy_from_uuid from course_main where pk1 = %s and copy_from_uuid is not null "
uuid_query = "select copy_from_uuid from course_main where uuid = %s "
uuid_to_courseid_query = "select course_id from course_main where uuid = %s "
find_dependants_recursive_query = "with recursive sub as (select ancestor_pk1, descendant_pk1 from x_course_contents where ancestor_pk1 = %s union select x.ancestor_pk1, x.descendant_pk1 from x_course_contents x inner join sub s on s.descendant_pk1 = x.ancestor_pk1 ) select * from sub; "
find_ancestors_recursive_query = "with recursive sub as ( select descendant_pk1, ancestor_pk1 from x_course_contents where descendant_pk1 = %s union select x.ancestor_pk1, x.descendant_pk1 from x_course_contents x inner join sub s on s.ancestor_pk1 = x.descendant_pk1 ) select * from sub; "
get_content_ancestors = "select  ancestor_pk1, descendant_pk1, distance from x_course_contents where descendant_pk1 = %s "
get_content_type = "select pk1, content_type, cnthndlr_handle, text_format_type from course_contents where pk1 = %s "
get_cms_resource_link = "select parent_pk1, crsmain_pk1, parent_data_type, resource_id from cms_resource_link where crsmain_pk1 = %s " 
get_xyf_url = "select file_name, full_path from xyf_urls where file_id = %s and full_path not like %s and full_path not like %s " 
get_all_xyf_urls_CNAME_match = "select file_name, full_path from xyf_urls where full_path like %s and full_path like %s"
get_all_xyf_urls_CNAME_match = "select file_name, full_path from xyf_urls where full_path like %s"
get_all_xyf_urls_CNAME_match = "select file_name, full_path from xyf_urls where full_path like %s and full_path not like %s "

# Provide course pk1 on command line
#crspk1 = sys.argv[1]

# DDA credentials
db = psycopg2.connect(database='xxxx', user='ddauser1', password='xxxxx', host='xxxx.edu', port='54320')
db_cms_doc = psycopg2.connect(database='xxxx_cms_doc', user='xxxx', password='xxxxxx', host='xxxxx.edu', port='54320')

cur = db.cursor()
cur_cms_doc = db_cms_doc.cursor()

# FUNC: get_all_xyf_urls(CNAME)
# fetches all of the URLS from xyf_urls where the full_path contains a reference/matches CNAME(course_id) 
def get_all_xyf_urls(CNAME):
        global CMS_RESOURCE_URLS
	global IMPORTED_CONTENT_LIST
        url_match_word = "%/internal/courses/%"
        url_outfile_path = "/var/tmp/" + CNAME + "_url.out"
        url_outfile = open(url_outfile_path, "w")
        
	cur_cms_doc.execute(get_all_xyf_urls_CNAME_match, (CNAME, url_match_word))
	#cur_cms_doc.execute(get_all_xyf_urls_CNAME_match, (CNAME, ))
        #rows = cur_cms_doc.fetchmany(1000)
        rows = cur_cms_doc.fetchall()
        print "# of URLS matching CNAME found in xyf_urls = ", len(rows)
        if rows != None :
                elem = "_ImportedContent_" 
		for row in rows:
			CMS_DOC_XYF_URLS.append(row[1])
			url_str = row[1]
                        #print url_str
                        if "_ImportedContent_" in url_str:
				elist = url_str.split('/')
                                for item in elist:
					if elem in item:
						prefix = item.split("_ImportedContent_", 1)
                                                pval = prefix[0]
		        			#print "prefix = ", prefix[1]
                                                if pval not in IMPORTED_CONTENT_LIST: 
							IMPORTED_CONTENT_LIST.append(pval)
			
                        
        url_outfile.write("******** CMS RESOURCE URLS LISTING: \n")
        url_outfile.write("\n".join(CMS_RESOURCE_URLS))
        url_outfile.write("\n\n\n******** XYF_URLS LISTING: \n")
        url_outfile.write("\n".join(CMS_DOC_XYF_URLS )) 
        url_outfile.close()

#FUNC: get_xyf_urls(RES_LIST)
def get_xyf_urls(RES_LIST, CNAME):
        INST_MATCH = "%institution%"
	for file_id in RES_LIST:
		#print "file_id = ", file_id  
		cur_cms_doc.execute(get_xyf_url, (file_id, CNAME, INST_MATCH, ))
		rows = cur_cms_doc.fetchone()
                if rows != None :
			#print "file=", rows[0]
			#print "url=", rows[1]
                        CMS_RESOURCE_URLS.append(rows[1])
                        #url_str = str(rows[1])
                        #print url_str.split('/')
			

#FUNC: get_cms_res(crspk1)
def get_cms_res(crspk1):
        global CMS_RESOURCE_IDS
        global CMS_RESOURCE_URLS
	cur.execute( get_cms_resource_link, (crspk1,))
        rows = cur.fetchall()
	print "# of resource_link(s) in cms_resource_links table = ", len(rows)
	if rows != None:
		for row in rows:
                        res_lnk = row[3]
			#strip off trailing "-1" from the res_lnk string
			raw_res_lnk = res_lnk.replace('_1', '')
			CMS_RESOURCE_IDS.append( raw_res_lnk )

#FUNC: uuid_to_courseid()
def uuid_to_courseid(uuidvalue):
	cur.execute( uuid_to_courseid_query, (uuidvalue, ))
        rows = cur.fetchone() 
	if rows != None:
		cuid = rows[0]
                return cuid
	
#FUNC: uuid_chain() returns a hiercy list of copied-from-uuid listing
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
		print "not copied from any other course/UUID"


# FUNC: get_course_id (short name) 
def get_course_id(crspk1):
	cur.execute( get_crs_name, (crspk1,) )
        rows = cur.fetchone()
        #print rows
	name = rows[0]
        return str(name)	

# FUNC: get_course_pk1()
def get_course_pk1(crs_name):
        print "CRS NAME = ", crs_name
        crs_nameA = str(crs_name)
	cur.execute( get_crs_id, (crs_nameA,))
	rows = cur.fetchone()
	crsmain_pk1 = rows[0]
	return str(crsmain_pk1)
	
      
# Function get ALL content for a given course_pk1
def all_contents(crspk1):
	global ALL_ITEMS
	cur.execute( all_content_items, (crspk1,) )
	rows = cur.fetchall()
	print "# of All content items = ", len(rows)
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
	print "# of Parent (folder) content items = ", len(rows)
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
	print "# ToC menu items = ", len(rows)
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
        	cname = get_course_id(crspk1) 
	else:
                cname = crspk1
                pk1 = get_course_pk1(crspk1)
                crspk1 = pk1
                

	ALL_ITEMS = []
        # parent_items = folders
	PARENT_ITEMS = []
        # toc_items = table of contents menu (top folder) items
	TOC_ITEMS = []
        # resource_id list from the cms_resource_link table belonging to the specified crsmain_pk1 course
	# NOTE: resource_id from cms_resource_link corresponds to file_id record in xyf_files table (in XXXX_cms_doc database)
	CMS_RESOURCE_IDS = []
        # a List of URLs corresponing to each resource_id in the CMS_RESOURCE_IDS list
        CMS_RESOURCE_URLS = []
        # list of full_path records from xyf_urls table 
        CMS_DOC_XYF_URLS = []
        #IMPORTED_CONTENT_LIST, a list of unique URLS which are imported into a course ( usually from other/older courses ) 
	IMPORTED_CONTENT_LIST = []
        # An initialized loop counter 
	REC_COUNT = 0

        print '\n'
        print "COURSE ID = ", cname
        print "COURSE PK1 = ", crspk1 

        cname_pattern = '%' + cname + '%'

	all_contents(crspk1)
	#print "ALL CONTENT = ", ALL_ITEMS

	all_parents(crspk1)
	#print "PARENT CONTENT = ", PARENT_ITEMS

        get_toc(crspk1) 
        #print "TOC LIST = ", TOC_ITEMS
        for tocitem in TOC_ITEMS:
		if tocitem not in ALL_ITEMS:
			print "toc item is NOT in content ", tocitem
        
        #Check if course was copied from another COURSE/UUID
	copy_from(crspk1)

        # Subtract parent_items (folders) from the ALL_ITEMS list of content items
        BASE_LIST = [x for x in ALL_ITEMS if x not in PARENT_ITEMS]
        NEW_BASE_LIST = [x for x in BASE_LIST if x not in TOC_ITEMS ]
        print "# of base (non-folder,non-ToC) content items  = ", len(NEW_BASE_LIST)

        get_cms_res(crspk1)
        #print "CMS_RESOURCE_IDS = ", CMS_RESOURCE_IDS

        get_xyf_urls(CMS_RESOURCE_IDS, cname_pattern)
        # print '\n'

        # this next step can take a 'long' time to return from the sql query
	# since it uses a pattern match of the course_id (aka CNAME) of the URL in the full_path column of the xyf_urls table in XXX_cms_doc database
        get_all_xyf_urls(cname_pattern)
        #print "IMPORTED_CONTENT_LIST = ", IMPORTED_CONTENT_LIST
	#for item in IMPORTED_CONTENT_LIST:
	#	print "ImportedContent =", item

##for citem in NEW_BASE_LIST:
	#print "item = ", citem 
######	get_handle( citem )
#        get_ancestors( citem )
        

#print '\n'
