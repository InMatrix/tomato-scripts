from pymongo import MongoClient, DESCENDING
import pdb
import gzip, os

__DBNAME__ = 'router_logs'
global client
client = MongoClient()

def update_dev_list(file_object, timestamp):
	"""Update the device list collection in the database with the info in a new device list file received.
	The function does the following for every device in the file:
		* creates a new record if the device is new or it is a known device but it has a different ip
		* do nothing if it's a known device with the same ip address
	The function then record the last update timestamp somewhere
	"""
	db = client[__DBNAME__]
	dev_list = db.dev_list
	for line in file_object:
		dev_record = line.split()
		dev_name = dev_record[3]
		dev_ip = dev_record[2]
		dev_rs = dev_list.find({'dev_name': dev_name})
		if dev_rs.count() == 0:
			# New device, add record
			new_dev = {'dev_name': dev_name, 'ip': dev_ip, 'timestamp': timestamp}
			dev_list.insert(new_dev)
		else:
			# Known device, then test if its IP has changed since last update.
			last_ip = dev_rs.sort('timestamp', DESCENDING)[0]['ip']
			if last_ip != dev_ip:
				new_dev = {'dev_name': dev_name, 'ip': dev_ip, 'timestamp': timestamp}
				dev_list.insert(new_dev) # Refactor
	# Update the timestamp of the most recently processed device list file
	update_latest_dev_list_timestamp(timestamp)
	return True

def update_latest_dev_list_timestamp(timestamp):
	db = client[__DBNAME__]
	db.prop.update({'key': 'latest_dev_list_timestamp'}, {'key': 'latest_dev_list_timestamp', 'val': timestamp}, upsert=True)

def get_latest_dev_list_timestamp():
	db = client[__DBNAME__]
	try:
		latest_dev_list_timestamp = db.prop.find_one({'key': 'latest_dev_list_timestamp'})['val']
	except TypeError:
		latest_dev_list_timestamp = 0
	return latest_dev_list_timestamp

def find_dev_name(ip, timestamp):
	""" Find a device's name based on its IP address and the time its IP address was logged. 
	1. In the dev_list collection, find all the devices that had used the ip before the timestamp.
	2. If there was only one device used that ip, return that device's name.
	3. If there were more than one device,  use the name of the device that used the IP address most recently.
	"""
	db = client[__DBNAME__]
	dev_list = db.dev_list
	rs = dev_list.find({'ip':ip, 'timestamp':{'$lte': timestamp}})
	rs_ct = rs.count()
	if rs_ct == 0:
		return 'unknown device'
	elif rs_ct == 1:
		return rs[0]['dev_name']
	else:
		return rs.sort('timestamp', DESCENDING)[0]['dev_name'] 

def write_access_log(file_object):
	""" This function writes to the domain access log based on a new recent domains log file received
	If the timestamp is larger than the latest timestamp in the dev_list, the dev_list needs to catch up, \
	do not process the file for now.
	Otherwise, the IP addresses in the original log file will be replaced by the device name obtained from find_dev_name.
	"""
	db = client[__DBNAME__]
	access_log = db.access_log
	latest_dev_list_timestamp = get_latest_dev_list_timestamp()
	try:
		latest_access_log_timestamp = access_log.find_one(sort=[('timestamp', DESCENDING)])['timestamp']
	except TypeError:
		latest_access_log_timestamp = 0
	# There can be more than one record updated with the same timestamp,
	# but for simplicity and the purpose of this application, 
	# we assume that a timestamp won't show up in more than one log file.
	for line in file_object:
		record = line.split()
		timestamp = int(record[0])
		domain = record[2]
		if timestamp <= latest_access_log_timestamp:
			# This line and all lines below it have been processed. 
			break
		ip = record[1]
		# Check if the timestamp is newer than the latest timestamp processed for the device list log.
		if timestamp <= latest_dev_list_timestamp:
			dev_name = find_dev_name(ip, timestamp)
			access_log.insert({'timestamp': timestamp, 'dev_name': dev_name, 'domain': domain})
		else:
			access_log.insert({'timestamp': timestamp, 'ip': ip, 'domain': domain})

def process_delayed_access_records():
	"""This function find device names for those access log records inserted before the device list has been updated. """
	db = client[__DBNAME__]
	access_log = db.access_log
	latest_dev_list_timestamp = get_latest_dev_list_timestamp()
	rs = access_log.find({'timestamp': {'$lte': latest_dev_list_timestamp}, 'ip': {'$exists': True}})
	for r in rs:
		r['dev_name'] = find_dev_name(r['ip'], r['timestamp'])
		r.pop('ip', None)
		access_log.update({'_id': r['_id']}, r)


def archive_log(f_in, filename):
	"""This function compresses a processed log file and move it to the archived folder"""
	if not os.path.isdir('archived'):
		os.makedirs('archived')
	f_out = gzip.open('archived/'+filename+'.gz', 'wb')
	f_out.writelines(f_in)
	f_out.close()
	f_in.close()











