#!/usr/bin/env
import sys
import json
import pymongo
from pymongo import MongoClient
from datetime import datetime
from delorean import Delorean, parse

client = MongoClient('192.168.1.6', 27017)
db = client.router_logs
ET = 'US/Eastern'

def query_db(start, end):
	"""start and end are two datetime.datetime instances with timezone info.
	"""
	dev_records = dict()
	pipeline = [
		{'$match': {
			'accessed_on': {'$gte': start, '$lte': end}, 
			}
		},
		{'$group': {
			'_id': {
					'dev_name': '$dev_name',
					'time':{ 
					 	'year': {'$year': '$accessed_on'}, 
					 	'month': {"$month": '$accessed_on'}, 
					 	'day': {"$dayOfMonth": '$accessed_on'}, 
					 	'hour': {"$hour": '$accessed_on'}
					 	} 
					},
			'count': {'$sum': 1}
			}
		},
		{'$sort': {'_id.time': 1}}
	]

	cursor = db.access_log.aggregate(pipeline, cursor={})
	total_hours = compute_hour_diff(start, end)

	for doc in cursor:
		dev_name = doc['_id']['dev_name']
		
		if dev_name not in dev_records:
			dev_records[dev_name] = [0] * total_hours # initialize the list
		
		t = doc['_id']['time']
		this_time = Delorean(datetime=datetime(t['year'], t['month'], t['day'], t['hour']), timezone='UTC')
		hour_diff = compute_hour_diff(start, this_time.datetime)
		dev_records[dev_name][hour_diff] = doc['count'] #TODO: Check overwriting

	return dev_records

def compute_hour_diff(t1, t2):
	elapsed_time = t2 - t1
	elapsed_hours = int(divmod(elapsed_time.total_seconds(), 3600)[0])
	return elapsed_hours

def normalize(counts):
	""" Transform the raw count of access records for each hour to (X - u) / u
		u is the mean of this day's hourly counts.
	"""
	non_zeros = [x for x in counts if x != 0]
	mean = sum(non_zeros) / float(len(non_zeros))
	scores = [ (x - mean) / mean for x in counts]
	return scores

if __name__ == '__main__':
	start_date = None
	end_date = None
	# start_date = Delorean(datetime(2014, 6, 5, 7), timezone = ET)
	# start_date.shift('UTC')
	# end_date = Delorean(datetime(2014, 6, 6, 7), timezone = ET)
	# end_date.shift('UTC')
	try:
		start_date = parse(sys.argv[1])
		end_date = parse(sys.argv[2])
	except:
		print "Usage: hour_range_norm.py <start_date> <end_date>"
		print "Example: hour_range_norm.py '2014-06-05 6:00 -0400' '2014-06-06 5:00 -0400'"
		print "Reference: http://delorean.readthedocs.org/en/latest/quickstart.html#strings-and-parsing"
		sys.exit(2)

	dev_records = query_db(start_date.datetime, end_date.datetime)

	for dev in dev_records:
		dev_records[dev] = normalize(dev_records[dev])

	output = {'metadata': 
		{'start_time': start_date.epoch(), 'end_time': end_date.epoch()},
		'dev_records': dev_records
	}
	print json.dumps(output)





