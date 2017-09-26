#!/usr/bin/python

########################################################################################
#
# Mike Laxer
#
# Script for finding datasets in CMR that should be unpublished from CMR.
# Works by finding public datasets in CMR that do not have datatset_state of
# "Published|Published Deferred QC|Published No QC" in the EDB.
#
#
#
#
########################################################################################

import io
import json
import sys
import psycopg2
import psycopg2.extras

with open('./json/nsidcv0_noauth_out.json','r') as v0_file:
	v0_json = json.load(v0_file)

with open('./json/nsidcecs_noauth_out.json','r') as ecs_file:
    ecs_json = json.load(ecs_file)


ecs_feed = ecs_json['feed']
ecs_entry = ecs_feed['entry']
v0_feed = v0_json['feed']
v0_entry = v0_feed['entry']

ecs_list = []
v0_list = []

i = 0
j = 0

while i < len(ecs_entry):
	if len(ecs_entry[i]["version_id"]) > 1:
		ecs_sname = ecs_entry[i]["short_name"] + '.' +  ecs_entry[i]["version_id"]
	else:
		ecs_sname =  ecs_entry[i]["short_name"] + '.00' +  ecs_entry[i]["version_id"]
	i += 1
	ecs_list.append(ecs_sname)
while j < len(v0_entry):
	if len(v0_entry[j]["version_id"]) > 1:
		v0_sname = v0_entry[j]["short_name"] + '.' +  v0_entry[j]["version_id"]
	else:
		v0_sname =  v0_entry[j]["short_name"] + '.00' +  v0_entry[j]["version_id"]
	j += 1
	v0_list.append(v0_sname)



cmr_list = set(ecs_list + v0_list)

con = None

try:
	con = psycopg2.connect(database='edb_prod', user='ops', host='db.production.edb.apps.int.nsidc.org', port='5432', password='ops')
	cursor= con.cursor(cursor_factory=psycopg2.extras.DictCursor)

	query = """
		select distinct authoritative_id||'.'||to_char(major_version, 'fm000') from internals inner join datasets on
		datasets.internal_id = internals.id inner join dataset_states on
		datasets.dataset_state_id = dataset_states.id where (dataset_states.name =
		'Published' or dataset_states.name = 'Published No QC' or
		dataset_states.name = 'Published Deferred QC') order by authoritative_id||'.'||to_char(major_version, 'fm000')
		"""
	cursor.execute(query) 
	query_out = cursor.fetchall()

except psycopg2.DatabaseError, e:
	print 'Error %s' % e
	sys.exit(1)
finally:
	if con:
		con.close()

q_results = list(query_out) 

try:
	con = psycopg2.connect(database='edb_prod', user='ops', host='db.production.edb.apps.int.nsidc.org', port='5432', password='ops')
	cursor= con.cursor(cursor_factory=psycopg2.extras.DictCursor)
	agdc_brokered_query = """
		select distinct authoritative_id||'.'||to_char(major_version, 'fm000') from internals inner join datasets on
		datasets.internal_id = internals.id inner join dataset_states on
		datasets.dataset_state_id = dataset_states.id inner join contacts_datasets
		on datasets.id = contacts_datasets.dataset_id inner join contacts on
		contacts.id = contacts_datasets.contact_id inner join organizations on
		organizations.id = contacts.organization_id inner join roles on roles.id =
		contacts_datasets.role_id inner join datasets_online_resources on
		datasets.id = datasets_online_resources.dataset_id inner join
		online_resources on datasets_online_resources.online_resource_id =
		online_resources.id inner join online_resource_types on
		online_resource_types.id = online_resources.online_resource_type_id 
		WHERE
		((organizations.short_name = 'AGDC' and roles.value = 'internal data center') 
		or (online_resource_types.sub_type = 'Brokered')) order by authoritative_id||'.'||to_char(major_version, 'fm000')
		"""
	cursor.execute(agdc_brokered_query)
	ab_query_out = cursor.fetchall()

except psycopg2.DatabaseError, e:
	print 'Error %s' % e
	sys.exit(1)

finally:
	if con:
		con.close()

ab_results = list(ab_query_out)
edb_list = []
ab_list = []

for q in q_results: edb_list.append(q[0])
for ab in ab_results: ab_list.append(ab[0])

ab_set = set(ab_list)
edb_pub = [e for e in edb_list if e not in ab_set]

matches = [c for c in cmr_list if c not in edb_list]

print "\nNumber of public V0/ECS datasets in CMR: %s" % len(cmr_list)
print "Number of published datasets in EDB: %s" % len(edb_list)
print "Number of published datasets in EDB (except brokered or AGDC): %s" % len(edb_pub)
print "Number of datasets public in CMR but not in a Published state in EDB: %s\n" % len(matches)
print "Investigate the following collections and determine if they should be unpublished from CMR:\n"
for m in sorted(matches): print m
