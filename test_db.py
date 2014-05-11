import log_db as ld
from pymongo import MongoClient, DESCENDING
import os, gzip
from datetime import datetime
import pdb

def setup_module():
	print("setup_module\n")
	ld.__DBNAME__ = 'test'
	global client
	client = MongoClient()
	global db
	db = client.test

def teardown_module():
	db.drop_collection('dev_list')
	db.drop_collection('access_log')
	db.drop_collection('prop')
	client.close()

class TestUpdateDevList:
	def setup_class(self):
		print 'setup_class'
		self.dev_list = db.dev_list

	def test_new_devs(self):
		filename = 'testdata/dnsmasq.leases_1397433316'
		timestamp = 1397433316
		my_date = datetime.utcfromtimestamp(timestamp)
		with open(filename) as f:
			ld.update_dev_list(f, timestamp)
		device = self.dev_list.find_one({"dev_name":"Taos-iPhone-5S"})
		count = self.dev_list.find({'last_updated': my_date}).count()
		dev_list_latest_updated = db.prop.find_one({'key': 'dev_list_latest_updated'})['val']
		assert device['ip'] == "192.168.1.23" and device['last_updated'] == my_date \
			and count == 7 and dev_list_latest_updated == my_date

	def test_update_devs(self):
		filename = 'testdata/dnsmasq.leases_1397437705'
		timestamp = 1397437705
		my_date = datetime.utcfromtimestamp(timestamp)
		with open(filename) as f:
			ld.update_dev_list(f, timestamp)
		tao_iphone = self.dev_list.find_one({"dev_name": "Taos-iPhone-5S"}, sort=[('last_updated', DESCENDING)])
		idea_pc = self.dev_list.find_one({"dev_name":"idea-PC"})
		latest_count = self.dev_list.find({'last_updated':my_date}).count()
		total_count = self.dev_list.find().count()
		dev_list_latest_updated = db.prop.find_one({'key': 'dev_list_latest_updated'})['val']
		assert tao_iphone['ip'] == "192.168.1.20" and tao_iphone['last_updated'] == my_date \
			and latest_count == 2 and total_count == 9 and idea_pc != None and dev_list_latest_updated == my_date

	def teardown_class(self):
		print 'teardown'
		db.drop_collection('dev_list')


class TestFindDevName:

	def setup_class(self):
		print 'setup_class'
		self.DEV_NAME = "Taos-iPhone-5S"
		self.dev_list = db.dev_list
		# setup the testing data
		self.record = {"accessed_on": datetime.utcfromtimestamp(1397437700), 'domain':'www.facebook.com', 'ip': '192.168.1.20'} 
		# setup the dev_list collection
		my_devs = [{"dev_name": self.DEV_NAME, 
					"ip": "192.168.1.20",
					"last_updated": datetime.utcfromtimestamp(1397437705)},
				   {"dev_name": "idea-PC", 
					"ip": "192.168.1.28",
					"last_updated": datetime.utcfromtimestamp(1397433316)}]

		self.dev_list.insert(my_devs)

	def test_unknown_dev(self):
		dev_name = ld.find_dev_name(self.record['ip'], self.record['accessed_on'])
		assert dev_name == None

	def test_single_dev(self):
		""" If there is a single record matching the given IP with an earlier timestamp in the database, 
		then get the dev_name from that record.
		"""
		new_dev = {	"dev_name": self.DEV_NAME,
				   	"ip": "192.168.1.20",
					"last_updated": datetime.utcfromtimestamp(1397433316)}
		self.dev_list.insert(new_dev)
		dev_name = ld.find_dev_name(self.record['ip'], self.record['accessed_on'])
		assert dev_name == self.DEV_NAME

	def test_multi_dev(self):
		""" If there are more than one record matching the given IP with an earlier timestamp in the database,
		then get the dev_name from the latest record. 
		"""
		new_dev = {	"dev_name": 'idea-PC',
		   	"ip": "192.168.1.20",
			"last_updated": datetime.utcfromtimestamp(1397435316)}
		self.dev_list.insert(new_dev)
		dev_name = ld.find_dev_name(self.record['ip'], self.record['accessed_on'])
		assert dev_name == 'idea-PC'

	def teardown_class(self):
		print 'teardown'
		db.drop_collection('dev_list')

class TestWriteAccessLog:

	def teardown_method(self, method):
		db.drop_collection('dev_list')
		db.drop_collection('access_log')
		db.drop_collection('prop')

	def test_process_delayed_access_records(self):
		# Prepare test data
		access_records = [{	"dev_name": "idea-PC", 
			"domain": "www.google.com",
			"accessed_on": datetime.utcfromtimestamp(1397433316)},
			{"dev_name": "idea-PC", 
			"domain": "www.facebook.com",
			"accessed_on": datetime.utcfromtimestamp(1397433316)},
			{"ip": "192.168.1.20", 
			"domain": "www.twitter.com",
			"accessed_on": datetime.utcfromtimestamp(1397437705)},
			{"ip": "192.168.1.20", 
			"domain": "www.twitter.com",
			"accessed_on": datetime.utcfromtimestamp(1397437710)},
			{"ip": "192.168.1.20", 
			"domain": "www.twitter.com",
			"accessed_on": datetime.utcfromtimestamp(1397437805)}]

		dev_list = [{"dev_name": 'iPhone-5S', 
			"ip": "192.168.1.20",
			"last_updated": datetime.utcfromtimestamp(1397437705)},
		   {"dev_name": "idea-PC", 
			"ip": "192.168.1.28",
			"last_updated": datetime.utcfromtimestamp(1397433316)}]
		
		latest_dev_list_time = datetime.utcfromtimestamp(1397437750)
		db.dev_list.insert(dev_list)
		db.access_log.insert(access_records)
		ld.update_dev_list_latest_updated(latest_dev_list_time)

		ld.process_delayed_access_records()
		r1 = db.access_log.find_one({"accessed_on": datetime.utcfromtimestamp(1397437705)})
		r2 = db.access_log.find_one({"accessed_on": datetime.utcfromtimestamp(1397437710)})
		r3 = db.access_log.find_one({"accessed_on": datetime.utcfromtimestamp(1397437805)})
		assert r1['dev_name'] == 'iPhone-5S' and r2['dev_name'] == 'iPhone-5S' \
			and 'ip' not in r1 and 'ip' not in r2 \
			and 'dev_name' not in r3 and r3['ip'] == "192.168.1.20"

	def test_write_access_log(self):
		'''This test expects:
		1) Only the first 9 lines in the log file are new and never existed in the database.
		2) The first 4 lines have timestamps later than the last time the dev_list was updated. 
		'''
		access_log = db.access_log
		# Prepare test data
		dev_list = [{"dev_name": 'MacBook-Air', 
			"ip": "192.168.1.40",
			"last_updated": datetime.utcfromtimestamp(1397415116)},
			{"dev_name": "iPhone-5S", 
			"ip": "192.168.1.23",
			"last_updated": datetime.utcfromtimestamp(1397415116)},
		   	{"dev_name": "idea-PC", 
			"ip": "192.168.1.49",
			"last_updated": datetime.utcfromtimestamp(1397415116)}]
		db.dev_list.insert(dev_list)

		access_records = [{"dev_name": "MacBook-Air", 
			"domain": "hmma.baidu.com",
			"accessed_on": datetime.utcfromtimestamp(1397415483)},
			{"dev_name": "MacBook-Air", 
			"domain": "www.google.com",
			"accessed_on": datetime.utcfromtimestamp(1397415483)}]
		access_log.insert(access_records)

		latest_dev_list_time = datetime.utcfromtimestamp(1397415499)
		ld.update_dev_list_latest_updated(latest_dev_list_time)

		# Call the function being tested
		with open('testdata/webmon_recent_domains_test') as f:
			ld.write_access_log(f)

		records_ct = access_log.find().count()
		# pdb.set_trace()
		records_delay_ct = access_log.find({'dev_name':{'$exists':False}, 'ip':{'$exists': True}}).count()

		assert records_ct == 11 and records_delay_ct == 4

def test_archive_log():
	filename = "test_archive_log.txt"
	gzip_filename = 'archived/'+filename+'.gz'
	f_in = open(filename,'w')
	f_in.write("abcdefg\nhijklmn")
	f_in.close()
	f_in = open(filename)
	ld.archive_log(f_in, filename)
	os.remove(filename)
	assert os.path.isfile(gzip_filename) and (not os.path.isfile(filename))
	os.remove(gzip_filename)
