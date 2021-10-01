# Python script to download Company Profiles from CHAPI
# Professor Alasdair Rutherford
# Created: 8 Feb 2021
# Modified: 1 Oct 2021


import chwrapper
import json
import csv
import os

from time import sleep

from progress import *

from urllib.error import HTTPError

from pathvalidate import sanitize_filename

def getprofile(client, comp):
	response = None
	active = True
	attempts=0
	while active==True:
		try:
			response = client.search_companies(comp)
			#print(response, response.status_code)
			#print(response.json())
			if response.status_code == 200:
				print('|')
				active=False
				payload = response.json()
				if payload['items'] == []:
					response = None
				httpcode = '<Response [200]>'
			else:
				print('/')
				sleep(2)
				attempts+=1

		except HTTPError as err:
			print('*', end='')
			sleep(1)
			attempts+=1
			if attempts>=3:
				active=False
				response = False
				httpcode = str(err.response)
				print('| ', err.response)			
		except:
			print('X', end='')
			sleep(0.5)
			attempts+=1
			if attempts>=3:
				active=False
				response = False
				httpcode = 'unknown'
				print('')

	return response, httpcode


#companynumber, p.json(), missing, p.status_code
def prepprofilerow(company, charity, payload, fieldnames, addressfields, miss, apistatus):

	outrow={}
	#print(payload)

	# Metadata
	outrow['apistatus'] = apistatus
	outrow['companyid'] = company
	outrow['charityid'] = charity
	outrow['fetchdate'] = fetchdate
	outrow['fetchedby'] = fetchedby


	# Main fields
	for f in fieldnames:
		outrow[f] = trydataextraction(f, payload, miss)

	# Address fields
	for f in addressfields:
		outrow[f] = trydataextraction(f, payload['address'], miss)

	return outrow


def trydataextraction(field, payload, miss):
	try:
		dataitem = payload[field]
	except:
		dataitem = miss

	return dataitem





# ============================================================================

# Main loop

# Companies House token 
tokenPath = 'tokens/chapitoken.txt' 
tokenFile = open(tokenPath, "r")
chapiToken = tokenFile.read()
search_client = chwrapper.Search(access_token=chapiToken)
tokenFile.close()


# Metadata
missing = '.'
fetchdate =  '1Oct2021'
fetchedby = 'AR'

# Input file
inputfilepath = "rawdata/ccew_company_numbers_david.csv"	#ccew_companynumbers.csv" 
with open(inputfilepath) as f:
	totalrows = sum(1 for line in f)
csvfile = open(inputfilepath, encoding="utf-8")
reader = csv.DictReader( csvfile)

#Output file
filename = 'csv/profiles_ew_' + fetchdate + 'v1.csv'
csvfile =  open(filename, 'w', newline='')

# Define fieldnames in output file
fieldnames = ['title', 'address_snippet', 'company_status', 'company_type', 'description','links', 'description', 'date_of_creation']
metadata = ['companyid', 'charityid', 'apistatus', 'fetchdate', 'fetchedby']
addressfields = ['postal_code', 'address_line_1', 'address_line_2', 'address_line_3', 'premises', 'locality']


outputfieldnames = fieldnames + metadata  + addressfields

# Open a file for HTTP errors
errorfilepath = 'rawdata/errors-chp-' + fetchdate + '.csv'
errorfile = open(errorfilepath, 'w', encoding="utf-8", newline='')
errfields = ("charityid", "companyid", "httperror")
errorwr = csv.DictWriter(errorfile, errfields)
errorwr.writeheader()


# Create blank record
blankrow = {}
for f in outputfieldnames:
	blankrow[f] = missing

blankrow['fetchdate'] = fetchdate
blankrow['fetchedby'] = fetchedby
blankrow['apistatus'] = 'Failed'


# Make directory for raw scrape
folder = 'chprofiles_' + fetchdate
if not os.path.exists(folder):
    os.makedirs(folder)


print(blankrow)


writerOutput = csv.DictWriter( csvfile, fieldnames=outputfieldnames)
writerOutput.writeheader()

print("Ready to go...")
progress = Progress(totalrows, datestamp = fetchdate, log=True)
#companylist = [{'companynumber': '00116713'}, {'companynumber': '00593878'}]

for row in reader:
 
	companynumber = row['companynumber'].strip()
	charityid = row['charityid']

	while len(companynumber)<8:
		companynumber = '0' + companynumber


	file = sanitize_filename('p' + str(companynumber) + '.txt')	
	filename = folder + '/' + file

	if not os.path.isfile(filename):

		print('---------------------------------------------------------')
		print(companynumber, end='')

		p, httpcode = getprofile(search_client, companynumber)
		#print(p.json())
		if p is not None:
			outrow = prepprofilerow(companynumber, charityid, p.json()['items'][0], fieldnames, addressfields, missing, p.status_code)
			#Output raw data to file
			textfile =  open(filename, 'w', newline='')
			data = p.json()
			json.dump(data, textfile)
			#textfile.write(p.json)
			textfile.close()
			writerOutput.writerow(outrow)
		else:
			blankrow['companyid'] = companynumber
			blankrow['charityid'] = charityid
			outrow = blankrow
			errorwr.writerow({'charityid': charityid, 'companyid': companynumber,'httperror': httpcode})
		#print(outrow)
	else:
		print('=', end='')

	progress.tick()
	print(progress.report())




