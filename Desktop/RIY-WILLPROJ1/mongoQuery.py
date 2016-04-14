import sys, getopt
from pymongo import MongoClient

def main(argv):
	fileList = [ '', '', '', '', '', '']
	query = {}
	skillQuery = {}
	interestQuery = {}
	userName = ['', '']
	user_id = ''
	client = MongoClient('localhost', 27017)
	db = client['collaboratornet']
	userCollection = db['Users']

	if len(argv) == 0:
		print ('Finding all users in database: \n')
		users = userCollection.find()
		for i in users:
			print i
			print '\n' 
	try:
		opts, args = getopt.getopt(argv, "hu:v:o:p:i:k:s:t:",["firstName=", "lastName=", 
					"organizationName=", "projectName=", "interestName=", "interestLevel="
					, "skillList=", "skillLevel="])

	except getopt.GetoptError:
		print 'See "python mongoQuery.py -h" for help on running program.'
		sys.exit(2)
	
	for opt, arg in opts:
		if opt == '-h':
			print 'Enter parameters to query for: \n'
			print '-u = query for first name \n'
			print '-v = query for last name \n'
			print '-o = query for organization \n'
			print '-p = query for project \n'
			print '-i = query for interest \n'
			print '-k = query for interest level\n'
			print '-s = query for skill \n'
			print '-t = query for skill level \n'
			print 'You may use any combination of these.\n'
			print 'Example:\n'
			print 'python mongoQuery -u <userName> -o <organizationName> \n'
			print 'will find all users of that name in that organization.\n'
			sys.exit()

		elif opt in ("-u", "--firstName"):
			userName[0] = arg
			query['first_name'] = userName[0]

		elif opt in ("-v", "--lastName"):
			userName[1] = arg
			query['last_name'] = userName[1]

		elif opt in ("-o", "--organizationName"):
			fileList[0] = arg
			query['organization'] = fileList[0]

		elif opt in ("-p", "--projectName"):
			fileList[1] = arg
			query['project'] = fileList[1]

		elif opt in ("-i", "--interestName"):
			fileList[2] = arg
			interestQuery['interest'] = fileList[2]

		elif opt in ("-k", "--interestLevel"):
			fileList[3] = arg
			to_int = int(fileList[3])
			interestQuery['interest_level'] = to_int

		elif opt in ("-s", "--skillList"):
			fileList[4] = arg
			skillQuery['skill'] = fileList[4]

		elif opt in ("-t", "--skillLevel"):
			fileList[5] = arg
			to_int = int(fileList[5])
			skillQuery['skill_level'] = to_int
	
	if fileList[0] != '' or fileList[1] != '' or userName != '':
		cursor = userCollection.find( query )
		for i in cursor:
			user_id = int(i['user_id'])
			print i

	if fileList[2] != '' or fileList[3] != '':
		interestCollection = db['Interests']
		if fileList[2] != '':
			interestQuery['interest'] = fileList[2]
		if fileList[3] != '':
			interestQuery['interest_level'] = fileList[3]
		if user_id != '':
			interestQuery['user_id'] = user_id
		cursor = interestCollection.find( interestQuery ) 
		for i in cursor:
			print i
	if fileList[4] != '' or fileList [5] != '':
		skillCollection = db['Skills']
		if fileList[4] != '':
			skillQuery['skill'] = fileList[4]
		if fileList[5] != '':
			skillQuery['skill_level'] = fileList[5]
		if user_id != '':
			skillQuery['user_id'] = user_id
		cursor = interestCollection.find( skillQuery ) 
		for i in cursor:
			print i

if __name__ == "__main__":
	main(sys.argv[1:])
