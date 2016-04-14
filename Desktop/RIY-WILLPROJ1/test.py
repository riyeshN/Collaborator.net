import csv
import json
import csv
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
from py2neo import Node, Relationship, Graph, authenticate
from py2neo.cypher import CypherError

graph = Graph()
authenticate("localhost:7474", "neo4j", "bigdatasexy")
graph = Graph("http://localhost:7474/db/data/")

cypher = graph.cypher
graph_constraints = {"User" : "user_id", "Organ" : "organization_name", "Proj" : "project_name", "Skills": "skill_name", "Interests": "interest_name"}

for k in graph_constraints:
    try:
        graph.schema.create_uniqueness_constraint(k, graph_constraints[k])
    except:
        pass

def inputusers(filename):
	# graph = Graph()
	# authenticate("localhost:7474", "neo4j", "bigdatasexy")
	# graph = Graph("http://localhost:7474/db/data/")

	with open(filename, 'rb') as file:
		dict = csv.DictReader(file, fieldnames=['user_id', 'first_name', 'last_name'])
		cypher = graph.cypher
	
		for row in dict:
			#cypher = graph.cypher
			try:
				archive = cypher.execute("CREATE (user:User {user_id:{a}, first_name:{b}, last_name:{c}}) RETURN user", a=row['user_id'], b=row['first_name'], c=row['last_name'])
			except CypherError as err:
				print "no can't do"
				return

def inputorg(filename):

	with open(filename, 'rb') as file:
		dict = csv.DictReader(file, fieldnames=['user_id', 'organization_name', 'organization_type'])
		cypher = graph.cypher

		for row in dict:
			#cypher = graph.cypher
			try:
				archive = cypher.execute("CREATE (orgn:Organ {organization_name:{a}, organization_type:{b}})", a=row['organization_name'], b=row['organization_type'])
			except CypherError as err:
				print "found repeted value...."
			try:
				archive = cypher.execute("MATCH (orgn:Organ {organization_name:{a}}), (user:User {user_id:{b}})" + "CREATE UNIQUE (user)-[rel:releted_to]->(orgn)", a=row['organization_name'], b=row['user_id'])

			except CypherError as err:
				print "can't do"
				return


def inputproject(filename):

	with open(filename, 'rb') as file:
		dict = csv.DictReader(file, fieldnames=['user_id', 'project_name'])
		cypher = graph.cypher

		for row in dict:
			#cypher = graph.cypher
			try:
				archive = cypher.execute("CREATE (project:Proj {project_name:{a}})", a=row['project_name'])
			except CypherError as err:
				print "found repeted value...."
			try:
				archive = cypher.execute("MATCH (project:Proj {project_name:{a}}), (user:User {user_id:{b}})" + "CREATE UNIQUE (user)-[rel:worked_in]->(project)", a=row['project_name'], b=row['user_id'])

			except CypherError as err:
				print "can't do"
				return

def inputskills(filename):
	with open(filename, 'rb') as file:
		dict = csv.DictReader(file, fieldnames=['user_id', 'skill_name', 'skill_weight'])
		cypher = graph.cypher
		

		for row in dict:
			try:
				row['skill_weight'] = float(row['skill_weight'])

			except ValueError:
				continue
			try:
				archive = cypher.execute("CREATE (skills:Skills {skill_name:{a}})", a=row['skill_name'])
			except CypherError as err:
				print "found repeted value...."
			try:
				archive = cypher.execute("MATCH (skills:Skills {skill_name:{a}}), (user:User {user_id:{b}})" + "CREATE UNIQUE (user)-[rel:has {weight: {c}}]->(skills)", a=row['skill_name'] , b=row['user_id'], c=row['skill_weight'])

			except CypherError as err:
				print "can't do"
				return 


def inputinterests(filename):
	with open(filename, 'rb') as file:
		dict = csv.DictReader(file, fieldnames=['user_id', 'interest_name', 'interest_weight'])
		cypher = graph.cypher
		

		for row in dict:
			try:
				row['interest_weight'] = float(row['interest_weight'])
				
			except ValueError:
				continue
			try:
				archive = cypher.execute("CREATE (interests:Interests {interest_name:{a}})", a=row['interest_name'])
			except CypherError as err:
				print "found repeted value...."
			try:
				archive = cypher.execute("MATCH (interests:Interests {interest_name:{a}}), (user:User {user_id:{b}})" + "CREATE UNIQUE (user)-[rel:is_into {weight: {c}}]->(interests)", a=row['interest_name'] , b=row['user_id'], c=row['interest_weight'])

			except CypherError as err:
				print "can't do"
				return 

def arrangedistance(filename):
	with open(filename, 'rb') as file:
		dict = csv.DictReader(file, fieldnames=['organization_1', 'organization_2', 'distance'])
		cypher = graph.cypher

		for row in dict:
			try:
				row['distance'] = float(row['distance'])
				
			except ValueError:
				continue
			try:
				archive = cypher.execute("MATCH (orgn1:Organ {organization_name:{a}}), (orgn2:Organ {organization_name:{b}})" + "CREATE UNIQUE (orgn1)-[rel:far {miles: {c}}]->(orgn2)", a=row['organization_1'], b=row['organization_2'], c = row['distance'])
			except CypherError as err:
				print "can't do"
				return 

def findusers(user_value):
	cypher = graph.cypher
	
	try:
		# archive = cypher.execute("MATCH (n:User {user_id: {a}})-[relate:releted_to]->(org:Organ)\n" + "MATCH (org:Organ)<-[r:far]->(b:Organ)\n" + "WHERE r.miles <= 10\n" + "MATCH (b:Organ)<-[relation_org:releted_to]-(users:User)\n" + "MATCH (users2:User)-[rel:releted_to]->(org:Organ)\n" + "MATCH (n:User)-[:has]->(likes)<-[knows:has]-(users:User)\n" + "MATCH (n:User)-[:has]->(likes)<-[knows2:has]-(users2:User)\n" + "RETURN collect(DISTINCT users.first_name), collect(DISTINCT users.last_name), collect(DISTINCT b.organization_name), knows.weight, collect(DISTINCT likes.skill_name)" + "ORDER BY knows.weight DESC", a = user_value)
		# archive1 = cypher.execute("MATCH (n:User {user_id: {a}})-[relate:releted_to]->(org:Organ)\n" + "MATCH (org:Organ)<-[r:far]->(b:Organ)\n" + "WHERE r.miles <= 10\n" + "MATCH (b:Organ)<-[relation_org:releted_to]-(users:User)\n" + "MATCH (users2:User)-[rel:releted_to]->(org:Organ)\n" + "MATCH (n:User)-[:has]->(likes)<-[knows:has]-(users:User)\n" + "MATCH (n:User)-[:has]->(likes)<-[knows2:has]-(users2:User)\n" + "RETURN collect(DISTINCT users2.first_name), collect(DISTINCT users2.last_name), collect(DISTINCT org.organization_name), knows2.weight, collect(DISTINCT likes.skill_name)" + "ORDER BY knows2.weight DESC", a = user_value)
		# archive3 = cypher.execute("MATCH (n:User {user_id: {a}})-[relate:releted_to]->(org:Organ)\n" + "MATCH (org:Organ)<-[r:far]->(b:Organ)\n" + "WHERE r.miles <= 10\n" + "MATCH (b:Organ)<-[relation_org:releted_to]-(users:User)\n" + "MATCH (users2:User)-[rel:releted_to]->(org:Organ)\n" + "MATCH (n:User)-[:is_into]->(likes)<-[knows:is_into]-(users:User)\n" + "MATCH (n:User)-[:is_into]->(likes)<-[knows2:is_into]-(users2:User)\n" + "RETURN collect(DISTINCT users2.first_name), collect(DISTINCT users2.last_name), collect(DISTINCT org.organization_name), knows2.weight, collect(DISTINCT likes.interest_name)" + "ORDER BY knows2.weight DESC", a = user_value)
		# archive2 = cypher.execute("MATCH (n:User {user_id: {a}})-[relate:releted_to]->(org:Organ)\n" + "MATCH (org:Organ)<-[r:far]->(b:Organ)\n" + "WHERE r.miles <= 10\n" + "MATCH (b:Organ)<-[relation_org:releted_to]-(users:User)\n" + "MATCH (users2:User)-[rel:releted_to]->(org:Organ)\n" + "MATCH (n:User)-[:is_into]->(likes)<-[knows:is_into]-(users:User)\n" + "MATCH (n:User)-[:is_into]->(likes)<-[knows2:is_into]-(users2:User)\n" + "RETURN collect(DISTINCT users.first_name), collect(DISTINCT users.last_name), collect(DISTINCT b.organization_name), knows.weight, collect(DISTINCT likes.interest_name)" + "ORDER BY knows.weight DESC", a = user_value)
		# print "Below is the info based on skills\n"
		# print(archive)
		# print "Below is the info base on skills who are in same organization\n"
		# print(archive1)
		# print "Below is the info based on interest\n"
		# print(archive2)
		# print "Below is the info based on interest who are in same organization\n"
		# print(archive3)
		archive = cypher.execute("MATCH (n:User {user_id: {a}})-[:is_into]->(interest)<-[interested_in:is_into]-(user:User)\n" + "MATCH (n:User)-[relate:releted_to]->(org:Organ)\n" + "MATCH (user:User)-[relat:releted_to]->(b:Organ)\n" + "MATCH (org:Organ)<-[r:far]->(b:Organ)\n" + "WHERE r.miles <= 10\n" + "MATCH (b:Organ)<-[relation_org:releted_to]-(users:User)\n" + "RETURN collect(DISTINCT user.first_name), collect(DISTINCT user.last_name), collect(DISTINCT interest.interest_name), interested_in.weight, collect(DISTINCT b.organization_name)", a=user_value)
		archive2 = cypher.execute("MATCH (n:User {user_id: {a}})-[:has]->(interest)<-[interested_in:has]-(user:User)\n" + "MATCH (n:User)-[relate:releted_to]->(org:Organ)\n" + "MATCH (user:User)-[relat:releted_to]->(b:Organ)\n" + "MATCH (org:Organ)<-[r:far]->(b:Organ)\n" + "WHERE r.miles <= 10\n" + "MATCH (b:Organ)<-[relation_org:releted_to]-(users:User)\n" + "RETURN collect(DISTINCT user.first_name), collect(DISTINCT user.last_name), collect(DISTINCT interest.skill_name), interested_in.weight, collect(DISTINCT b.organization_name)", a=user_value)
		print "\n"
		print "BELOW IS THE VALUED FOR MATCH BASED ON INTEREST\n"
		print(archive)
		print "\n"
		print "BELOW IS THE VALUED FOR MATCH BASED ON SKILL\n"
		print(archive2)
		print "\n"
	except CypherError as err:
		print "no can't do"
		return 

def project(user_value):
	cypher = graph.cypher
	try:
		archive = cypher.execute("MATCH (user:User {user_id: {a}}), (user1:User)-[:worked_in]->(proj)<-[:worked_in]-(user2:User), (usr1)-[:is_into]-(interest)\n" + "WITH user, user1, user2, proj, collect(interest.interest_name) as i\n" + "WHERE user1.user_id <> user.user_id AND user2.user_id <> user.user_id AND user1 <> user2\n" + "RETURN DISTINCT user1.first_name, i", a=user_value)
		print(archive)                 
	except CypherError as err:
		print "can't do"
    	return

			


