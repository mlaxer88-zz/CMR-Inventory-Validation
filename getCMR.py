#!/usr/bin/python

import requests
import os


def getToken(src):
	xml = open(src).read()
	response = requests.post(url='https://api.echo.nasa.gov/echo-rest/tokens', data=xml, headers={'Content-Type':'application/xml'}).text
	token = response.split("\n")[2][6:-5]
	return token


def getJson():
	v0Token = getToken('auth/token_nsidcV0.txt')
	ecsToken = getToken('auth/token_nsidcecs.txt')

	v0authHeader = {}
	v0authHeader['Echo-Token'] = v0Token
	v0authHeader['Accept'] = 'application/json'

	ecsauthHeader = {}
	ecsauthHeader['Echo-Token'] = ecsToken
	ecsauthHeader['Accept'] = 'application/json'

	v0noAuth = requests.get(url='https://cmr.earthdata.nasa.gov/search/collections.json?provider_short_name=NSIDCV0&page_size=1000&pretty=true')
	v0Auth = requests.get(url='https://cmr.earthdata.nasa.gov/search/collections.json?provider_short_name=NSIDCV0&page_size=1000&pretty=true', headers=v0authHeader)
	
	ecsnoAuth = requests.get(url='https://cmr.earthdata.nasa.gov/search/collections.json?provider_short_name=NSIDC_ECS&page_size=1000&pretty=true')
	ecsAuth = requests.get(url='https://cmr.earthdata.nasa.gov/search/collections.json?provider_short_name=NSIDC_ECS&page_size=1000&pretty=true', headers=ecsauthHeader)

	v0Out = open('json/nsidcv0_noauth_out.json', 'w')
	v0AuthOut = open('json/nsidcv0_auth_out.json', 'w')
	ecsOut = open('json/nsidcecs_noauth_out.json', 'w')
	ecsAuthOut= open('json/nsidcecs_auth_out.json', 'w')

	v0Out.write(v0noAuth.content)
	v0AuthOut.write(str(v0Auth.content))
	ecsOut.write(str(ecsnoAuth.content))
	ecsAuthOut.write(str(ecsAuth.content))
	

getJson()

	
