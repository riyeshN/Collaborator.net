import sys, getopt, pprint
import helpers
import test
from pymongo import MongoClient
from py2neo import Node, Relationship, Graph, authenticate

def main(argv): 
	usersFile = ''
	organizationsFile = ''
	projectFile = ''
	skillsFile = ''
	interestsFile = ''
	distancesFile = ''

	if len(argv) == 0:
		print 'Error: Not enough arguments. Type "python generateDB.py -h" for help.'
		sys.exit()
	
	client = MongoClient('localhost', 27017)
	db = client['collaboratornet']	

	try:
		opts, args = getopt.getopt(argv, "h", ["users=", "organizations=", "projects=", "skills=", "interests=", "distances="])
	except getopt.GetoptError:
		sys.exit()
	
	for opt, arg in opts:
		if opt == '-h':
			print 'Run program with "python generateDB.py <users>.csv <organizations>.csv <projects>.csv <skills>.csv <interests>.csv <distances>.csv".'
			sys.exit()

	print('Reading: users')
	usersFile = sys.argv[1]
	helpers.storeUsers(usersFile, db)
	test.inputusers(usersFile)

	print('Reading: organizations')
	organizationsFile = sys.argv[2]
	helpers.updateOrganization(organizationsFile, db)
	test.inputorg(organizationsFile)

	print('Reading: projects')
	projectFile = sys.argv[3]
	helpers.updateProjects(projectFile, db)
	test.inputproject(projectFile)

	print('Reading: skills')
	skillsFile = sys.argv[4]
	helpers.storeSkills(skillsFile, db)
	test.inputskills(skillsFile)

	print('Reading: interests')
	interestsFile = sys.argv[5]
	helpers.storeInterests(interestsFile, db)
	test.inputinterests(interestsFile)

	print('Reading: distances')
	distancesFile = sys.argv[6]
	helpers.storeDistances(distancesFile, db)
	test.arrangedistance(distancesFile)
	
	user_id_value = raw_input("Thank you for waiting!\nAll data are stored in!\nPlease insert the user_id to find users who closely match your interest/skills and are close to you:")
	test.findusers(user_id_value)
	test.project(user_id_value)


if __name__ == "__main__":
	main(sys.argv[1:])
