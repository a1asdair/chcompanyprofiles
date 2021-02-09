# Python script to download Company Profiles from CHAPI
# Professor Alasdair Rutherford
# Created: 8 Feb 2021


import chwrapper
import json
import csv
import os

from time import sleep



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
			else:
				print('/')
				sleep(2)
				attempts+=1
		except:
			print('*', end='')
			sleep(2)
			attempts+=1
			if attempts>=2:
				active=False
				response = None
				print('')

	return response


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
fetchdate =  '9Feb2021'
fetchedby = 'AR'

# Input file
inputfilepath = "csv/ccew_company_numbers_david.csv"	#ccew_companynumbers.csv" 
csvfile = open(inputfilepath, encoding="utf-8")
reader = csv.DictReader( csvfile)

#Output file
filename = 'csv/profiles_ew_9Feb21v1.csv'
csvfile =  open(filename, 'w', newline='')

# Define fieldnames in output file
fieldnames = ['title', 'address_snippet', 'company_status', 'company_type', 'description','links', 'description', 'date_of_creation']
metadata = ['companyid', 'charityid', 'apistatus', 'fetchdate', 'fetchedby']
addressfields = ['postal_code', 'address_line_1', 'address_line_2', 'address_line_3', 'premises', 'locality']


outputfieldnames = fieldnames + metadata  + addressfields

# Create blank record
blankrow = {}

for f in outputfieldnames:
	blankrow[f] = missing

blankrow['fetchdate'] = fetchdate
blankrow['fetchedby'] = fetchedby
blankrow['apistatus'] = 'Failed'

print(blankrow)


writerOutput = csv.DictWriter( csvfile, fieldnames=outputfieldnames)
writerOutput.writeheader()

print("Ready to go...")
#companylist = [{'companynumber': '00116713'}, {'companynumber': '00593878'}]

for row in reader:
 
	companynumber = row['companynumber'].strip()
	charityid = row['charityid']

	while len(companynumber)<8:
		companynumber = '0' + companynumber

	filename = 'chprofiles/p' + str(companynumber) + '.txt'

	if not os.path.isfile(filename):

		print('---------------------------------------------------------')
		print(companynumber, end='')

		p = getprofile(search_client, companynumber)
		#print(p.json())
		if p is not None:
			outrow = prepprofilerow(companynumber, charityid, p.json()['items'][0], fieldnames, addressfields, missing, p.status_code)
			#Output raw data to file
			textfile =  open(filename, 'w', newline='')
			data = p.json()
			json.dump(data, textfile)
			#textfile.write(p.json)
			textfile.close()
		else:
			blankrow['companyid'] = companynumber
			blankrow['charityid'] = charityid
			outrow = blankrow
		#print(outrow)
		writerOutput.writerow(outrow)
	else:
		print('=', end='')




