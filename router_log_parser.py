from pymongo import MongoClient, DESCENDING
import pdb
import gzip
import os
from datetime import datetime


class RouterLogParser:

    def __init__(self, home_id, db_conn_settings, db_name="home_trivia"):
        self.home_id = home_id
        self.client = MongoClient(**db_conn_settings)
        self.db = self.client[db_name]
        self.dev_list = self.db[home_id + "_dev_list"]
        self.access_log = self.db[home_id + "_access_log"]
        self.prop = self.db[home_id + "_prop"]

    def update_dev_list(self, file_object, timestamp):
        """Update the device list collection in the database with the info in a new device list file received.
        The function does the following for every device in the file:
            * creates a new record if the device is new or it is a known device but it has a different ip
            * do nothing if it's a known device with the same ip address
        The function then record the last update timestamp somewhere
        """
        dev_list = self.dev_list
        my_date = datetime.utcfromtimestamp(timestamp)

        for line in file_object:
            dev_record = line.split()
            dev_name = dev_record[3]
            dev_ip = dev_record[2]
            dev_rs = dev_list.find({'dev_name': dev_name})
            if dev_rs.count() == 0:
                # New device, add record
                new_dev = {'dev_name': dev_name, 'ip': dev_ip, 'last_updated': my_date}
                dev_list.insert(new_dev)
            else:
                # Known device, then test if its IP has changed since last update.
                last_ip = dev_rs.sort('last_updated', DESCENDING)[0]['ip']
                if last_ip != dev_ip:
                    new_dev = {'dev_name': dev_name, 'ip': dev_ip, 'last_updated': my_date}
                    dev_list.insert(new_dev)  # Refactor
        # Update the timestamp of the most recently processed device list file
        self.update_dev_list_latest_updated(my_date)
        return True

    def update_dev_list_latest_updated(self, my_date):
        self.prop.update(
            {'key': 'dev_list_latest_updated'},
            {'key': 'dev_list_latest_updated', 'val': my_date},
            upsert=True)

    def get_dev_list_latest_updated(self):
        prop = self.prop
        try:
            dev_list_latest_updated = prop.find_one({'key': 'dev_list_latest_updated'})['val']
        except TypeError:
            dev_list_latest_updated = datetime.utcfromtimestamp(0)
        return dev_list_latest_updated

    def find_dev_name(self, ip, my_date):
        """ Find a device's name based on its IP address and the time its IP address was logged.
        1. In the dev_list collection, find all the devices that had used the ip before the timestamp.
        2. If there was only one device used that ip, return that device's name.
        3. If there were more than one device, use the name of the device that used the IP address most recently.
        4. If no record exists for this ip before the specified datetime, return nothing.
        """
        dev_list = self.dev_list
        rs = dev_list.find({'ip': ip, 'last_updated': {'$lte': my_date}})
        rs_ct = rs.count()
        if rs_ct == 0:
            return None
        elif rs_ct == 1:
            return rs[0]['dev_name']
        else:
            return rs.sort('last_updated', DESCENDING)[0]['dev_name']

    def write_access_log(self, file_object):
        """ This function writes to the domain access log based on a new recent domains log file received
        If the timestamp is larger than the latest timestamp in the dev_list, the dev_list needs to catch up,
        do not process the file for now.
        Otherwise, the IP addresses in the original log file will be replaced by the device name obtained from find_dev_name.
        """
        access_log = self.access_log
        dev_list_latest_updated = self.get_dev_list_latest_updated()
        try:
            latest_access_log_time = access_log.find_one(sort=[('accessed_on', DESCENDING)])['accessed_on']
        except TypeError:
            latest_access_log_time = datetime.utcfromtimestamp(0)
        # There can be more than one record updated with the same timestamp,
        # but for simplicity and the purpose of this application,
        # we assume that a timestamp won't show up in more than one log file.
        for line in file_object:
            record = line.split()
            my_date = datetime.utcfromtimestamp(int(record[0]))
            if my_date <= latest_access_log_time:
                # This line and all lines below it have been processed.
                break
            ip = record[1]
            # Check if the timestamp is newer than the latest timestamp processed for the device list log.
            my_doc = {'accessed_on': my_date}
            if my_date <= dev_list_latest_updated:
                dev_name = self.find_dev_name(ip, my_date)
                if dev_name is not None:
                    my_doc['dev_name'] = dev_name
                else:
                    my_doc['ip_last'] = ip.split('.')[-1]
            else:
                my_doc['ip'] = ip
            access_log.insert(my_doc)

    def process_delayed_access_records(self):
        """This function find device names for those access log records
         inserted before the device list has been updated."""
        access_log = self.access_log
        dev_list_latest_updated = self.get_dev_list_latest_updated()
        rs = access_log.find(
            {'accessed_on':
                {'$lte': dev_list_latest_updated},
                'ip': {'$exists': True}}
        )
        for r in rs:
            r['dev_name'] = self.find_dev_name(r['ip'], r['accessed_on'])
            r.pop('ip', None)
            access_log.update({'_id': r['_id']}, r)

    def archive_log(self, f_in, filename):
        """This function compresses a processed log file and move it to the archived folder"""
        if not os.path.isdir('archived'):
            os.makedirs('archived')
        f_out = gzip.open('archived/'+filename+'.gz', 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
