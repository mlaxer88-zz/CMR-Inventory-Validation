#!/usr/bin/python

#################################################################################################################
# CmrIvtCore.py
#
# Maintainer: Mike Laxer
# Date: 2017-07-13
#
#
# Purpose: Inventory validation between the EDB and CMR (Common Metadata Repository)
#    Finds datasets that:
#     -Need to be published to CMR via the CMR Mediator
#     -Need to be unpublished from CMR because they're not in a Published state in the EDB
#     -Need to be unpublished from CMR's NSIDCV0 provder because they're already in the NSIDC_ECS provider
#     -Are not in the whitelist
#
# Requirements:
#    -psycopg2
#    -getCMR.py (should be in the same directory as this script)
#    -requests (For getCMR.py)
#
#
#################################################################################################################

import io
import json
import sys
import psycopg2
import psycopg2.extras
from getCMR import getJson

# Invoke getCMR.py to retrieve CMR results in JSON format and save to four files in ./json/
getJson()

with open('./json/nsidcv0_auth_out.json','r') as V0AuthFile:
    V0AuthJson = json.load(V0AuthFile)

with open('./json/nsidcecs_auth_out.json','r') as EcsAuthFile:
    EcsAuthJson = json.load(EcsAuthFile)

with open('./json/nsidcv0_noauth_out.json','r') as V0File:
    V0Json = json.load(V0File)

with open('./json/nsidcecs_noauth_out.json','r') as EcsFile:
    EcsJson = json.load(EcsFile)

# Getting JSON elements:
EcsAuthFeed = EcsAuthJson['feed']
EcsAuthEntry = EcsAuthFeed['entry']
V0AuthFeed = V0AuthJson['feed']
V0AuthEntry = V0AuthFeed['entry']

# Getting nested JSON elements:
EcsFeed = EcsJson['feed']
EcsEntry = EcsFeed['entry']
V0Feed = V0Json['feed']
V0Entry = V0Feed['entry']

EcsAuthList = []
V0AuthList = []
EcsList = []
V0List = []

i = j = k = m = 0

# Retrieve short names and version ID's and concatenate them:
while i < len(EcsAuthEntry):
    if len(EcsAuthEntry[i]["version_id"]) > 1:
        EcsAuthSname = EcsAuthEntry[i]["short_name"] + '.' + EcsAuthEntry[i]["version_id"]
    else:
        EcsAuthSname = EcsAuthEntry[i]["short_name"] + '.00' + EcsAuthEntry[i]["version_id"]
    i += 1
    EcsAuthList.append(EcsAuthSname)

while j < len(V0AuthEntry):
    if len(V0AuthEntry[j]["version_id"]) > 1:
        V0AuthSname = V0AuthEntry[j]["short_name"] + '.' + V0AuthEntry[j]["version_id"]
    else:
        V0AuthSname = V0AuthEntry[j]["short_name"] + '.00' + V0AuthEntry[j]["version_id"]
    j += 1
    V0AuthList.append(V0AuthSname)

while k < len(EcsEntry):
    if len(EcsEntry[k]["version_id"]) > 1:
        EcsSname = EcsEntry[k]["short_name"] + '.' + EcsEntry[k]["version_id"]
    else:
        EcsSname = EcsEntry[k]["short_name"] + '.00' + EcsEntry[k]["version_id"]
    k += 1
    EcsList.append(EcsSname)

while m < len(V0Entry):
    if len(V0Entry[m]["version_id"]) > 1:
        V0Sname = V0Entry[m]["short_name"] + '.' + V0Entry[m]["version_id"]
    else:
        V0Sname = V0Entry[m]["short_name"] + '.00' + V0Entry[m]["version_id"]
    m += 1
    V0List.append(V0Sname)

# Find collections common to both ECS and V0 lists:
InECSandV0 = list(set(EcsAuthList) & set(V0AuthList))
CmrAuthList = set(EcsAuthList + V0AuthList)
CmrList = set(EcsList + V0List)

db = 'edb_prod'
user = 'ops'
pw = 'ops'
port = '5432'
host = 'db.production.edb.apps.int.nsidc.org'

con = None

# Query EDB for published datasets
try:
    con = psycopg2.connect(database=db, user=user, host=host, port=port, password=pw)
    cursor= con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """
        SELECT DISTINCT
	    authoritative_id||'.'||to_char(major_version, 'fm000')
        FROM
	    internals
        INNER JOIN datasets ON datasets.internal_id = internals.id
        INNER JOIN dataset_states ON datasets.dataset_state_id = dataset_states.id
        WHERE
	    (dataset_states.name = 'Published'
        OR
	    dataset_states.name = 'Published No QC'
        OR
	    dataset_states.name = 'Published Deferred QC')
        ORDER BY
	    authoritative_id||'.'||to_char(major_version, 'fm000')
        """
    cursor.execute(query)
    QueryOut = cursor.fetchall()

except psycopg2.DatabaseError, e:
    print 'Error %s' % e
    sys.exit(1)

finally:
    if con:
        con.close()

# Query EDB for AGDC and Brokered datasets
try:
    con = psycopg2.connect(database=db, user=user, host=host, port=port, password=pw)
    cursor= con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    AgdcBrokeredQuery = """
        SELECT DISTINCT
	    authoritative_id||'.'||to_char(major_version, 'fm000')
        FROM
	    internals
        INNER JOIN datasets ON datasets.internal_id = internals.id
        INNER JOIN dataset_states ON datasets.dataset_state_id = dataset_states.id
        INNER JOIN contacts_datasets ON datasets.id = contacts_datasets.dataset_id
        INNER JOIN contacts ON contacts.id = contacts_datasets.contact_id
        INNER JOIN organizations ON organizations.id = contacts.organization_id
        INNER JOIN roles ON roles.id = contacts_datasets.role_id
        INNER JOIN datasets_online_resources ON datasets.id = datasets_online_resources.dataset_id
        INNER JOIN online_resources ON datasets_online_resources.online_resource_id = online_resources.id
        INNER JOIN online_resource_types ON online_resource_types.id = online_resources.online_resource_type_id
        WHERE
	    ((organizations.short_name = 'AGDC' AND roles.value = 'internal data center')
        OR
	    (online_resource_types.sub_type = 'Brokered'))
        ORDER BY
	    authoritative_id||'.'||to_char(major_version, 'fm000')
        """
    cursor.execute(AgdcBrokeredQuery)
    AgdcQueryOut = cursor.fetchall()

except psycopg2.DatabaseError, e:
    print 'Error %s' % e
    sys.exit(1)

finally:
    if con:
        con.close()

QueryResult = list(QueryOut)
AgdcBrokeredResult = list(AgdcQueryOut)
EdbList = []
AgdcBrokeredList = []

for q in QueryResult: EdbList.append(q[0])
for ab in AgdcBrokeredResult: AgdcBrokeredList.append(ab[0])

AgdcBrokeredSet = set(AgdcBrokeredList)

# Getting published collections in EDB, minus AGDC and brokered collections:
EdbPub = [e for e in EdbList if e not in AgdcBrokeredSet]

# Retrieve Whitelist:
Whitelist = []
wlFile = 'CmrIvtWhitelist.txt'
wl = open(wlFile, 'r')

for line in wl:
    Whitelist.append(line.strip('\n'))
wl.close()

# The lists we really care about:
EdbNotInCmr = [c for c in EdbPub if c not in list(CmrAuthList) + Whitelist]
CmrNotInEdb = [c for c in CmrList if c not in EdbList + Whitelist]
InV0AndInEcs = list(set(EcsList) & set(V0List))
InV0AndInEcs = [c for c in InV0AndInEcs if c not in Whitelist]
EdbNotPublicInCmr = [c for c in EdbPub if c not in list(CmrList) + Whitelist]

# Print results:
print "\nNumber of V0 and ECS datasets in CMR: %s" % len(CmrAuthList)
print "Number of public V0 and public ECS datasets in CMR: %s" % len(CmrList)
print "Number of published datasets in EDB: %s" % len(EdbList)
print "Number of published datasets in EDB (except brokered or AGDC): %s\n" % len(EdbPub)
print "Number of datasets published in EDB but not published to CMR: %s" % len(EdbNotInCmr)
print "Number of datasets public in CMR but not in a published state in EDB: %s" % len(CmrNotInEdb)
print "Number of datasets in public in EDB but not visible to unauthorized users: %s" % len(EdbNotPublicInCmr)
print "Number of collections in both NSIDC_ECS and NSIDCV0 providers in CMR: %s\n" % len(InV0AndInEcs)

# Print whitelist contents, if any:
if len(Whitelist) > 0:
    print "\nWhitelisted collections:\n"
    for c in Whitelist: print c
else:
    print "\nNo collections in the whitelist."

# If there's any result, exit with system code 1, otherwise 0:
if len(CmrNotInEdb) or len(EdbNotInCmr) or len(InV0AndInEcs) > 0:
    print "-" * 15
    print "\nInvestigate the following collections and determine if they should be published to CMR:\n"
    for m in sorted(EdbNotInCmr): print m
    print "-" * 15
    print "\nInvestigate the following collections and determine if they should be unpublished from CMR since they are not in a Published state in the EDB:\n"
    for m in sorted(CmrNotInEdb): print m
    print "-" * 15
    print "\nInvestigate the following collections and determine if they should be unpublished from NSIDCV0 since they are also in NSIDC_ECS:\n"
    for m in sorted(InV0AndInEcs): print m
    print "-" * 15
    print "\nInvestigate the following collections and determine if they should have their ACLs opened so they are visible in an unauthenticated CMR search:\n"
    for m in sorted(EdbNotPublicInCmr): print m
    print "-" * 15
    sys.exit(1)
else:
    print "\nNo collections found.  No further action required."
    sys.exit(0)
