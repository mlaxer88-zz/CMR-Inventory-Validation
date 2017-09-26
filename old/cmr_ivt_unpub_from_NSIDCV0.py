#!/usr/bin/python

#########################################################################################
#											#
# cmr_ivt_unpub_from_NSIDCV0.py								#
#											#
# A script to check for datasets in CMR that are in NSIDC_ECS and NSIDCV0		#
# Should be done from outside user perspective (not logged into EDSC)			#
#											#
# 2017/07/12										#
#											#
# Mike Laxer										#
#											#
#											#
#											#
#											#
#											#
#########################################################################################

import io
import json

with open('./json/nsidcecs_noauth_out.json','r') as ecs_file:
	ecs_json = json.load(ecs_file)

with open('./json/nsidcv0_noauth_out.json','r') as v0_file:
	v0_json = json.load(v0_file)

ecs_feed = ecs_json['feed']
ecs_entry = ecs_feed['entry']
v0_feed = v0_json['feed']
v0_entry = v0_feed['entry']

ecs_list = []
v0_list = []

i = 0
while i < len(ecs_entry):
	if len(ecs_entry[i]["version_id"]) > 1:
		ecs_sname = ecs_entry[i]["short_name"] + '.' +  ecs_entry[i]["version_id"]
	else:
		ecs_sname =  ecs_entry[i]["short_name"] + '.00' +  ecs_entry[i]["version_id"]
	i += 1
	ecs_list.append(ecs_sname)
j = 0
while j < len(v0_entry):
	if len(v0_entry[j]["version_id"]) > 1:
		v0_sname = v0_entry[j]["short_name"] + '.' +  v0_entry[j]["version_id"]
	else:
		v0_sname =  v0_entry[j]["short_name"] + '.00' +  v0_entry[j]["version_id"]
	j += 1
	v0_list.append(v0_sname)


matches = list(set(ecs_list) & set(v0_list)) 

print "Found %s public collections in both the NSIDC_ECS and NSIDCV0 providers on CMR.\n" % len(matches)
print "Investigate the following collections and determine if they should be unpublished from the NSIDCV0 provider in CMR:"
for m in matches: print m
