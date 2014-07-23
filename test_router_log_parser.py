from __future__ import print_function
import pytest

from router_log_parser import RouterLogParser
from pymongo import DESCENDING
# import os, gzip
from datetime import datetime
import pdb


@pytest.fixture(scope="class")
def log_parser(request):
    log_parser = RouterLogParser("h00", {"host": "mongodb://localhost/test"})

    def fin():
        log_parser.dev_list.drop()
        log_parser.access_log.drop()
        log_parser.prop.drop()

    request.addfinalizer(fin)
    return log_parser


class TestUpdateDevList():

    def test_new_devs(self, log_parser):
        filename = 'testdata/dnsmasq.leases_1397433316'
        timestamp = 1397433316
        my_date = datetime.utcfromtimestamp(timestamp)
        with open(filename) as f:
            log_parser.update_dev_list(f, timestamp)
        device = log_parser.dev_list.find_one({'hid': log_parser.home_id, "dev_name": "Taos-iPhone-5S"})
        count = log_parser.dev_list.find({'hid': log_parser.home_id, 'last_updated': my_date}).count()
        dev_list_latest_updated = log_parser.prop.find_one(
            {'hid': log_parser.home_id,
             'key': 'dev_list_latest_updated'}
        )['val']
        assert device['ip'] == "192.168.1.23" and device['last_updated'] == my_date \
            and count == 7 and dev_list_latest_updated == my_date

    def test_update_devs(self, log_parser):
        filename = 'testdata/dnsmasq.leases_1397437705'
        timestamp = 1397437705
        my_date = datetime.utcfromtimestamp(timestamp)
        with open(filename) as f:
            log_parser.update_dev_list(f, timestamp)
        tao_iphone = log_parser.dev_list.find_one(
            {'hid': log_parser.home_id,
             "dev_name": "Taos-iPhone-5S"},
            sort=[('last_updated', DESCENDING)]
        )
        idea_pc = log_parser.dev_list.find_one({'hid': log_parser.home_id, "dev_name": "idea-PC"})
        latest_count = log_parser.dev_list.find({'hid': log_parser.home_id, 'last_updated': my_date}).count()
        total_count = log_parser.dev_list.find().count()
        dev_list_latest_updated = log_parser.prop.find_one(
            {'hid': log_parser.home_id,
             'key': 'dev_list_latest_updated'}
        )['val']
        assert tao_iphone['ip'] == "192.168.1.20" and tao_iphone['last_updated'] == my_date \
            and latest_count == 2 and total_count == 9 and idea_pc is not None \
            and dev_list_latest_updated == my_date


class TestFindDevName():

    @pytest.fixture(scope="class")
    def access_record(self, log_parser):
        return {'hid': log_parser.home_id, "accessed_on": datetime.utcfromtimestamp(1397437700),
                'domain': 'www.facebook.com', 'ip': '192.168.1.20'}

    @pytest.fixture(scope="class")
    def test_dev_name(self):
        return "Taos-iPhone-5S"

    @pytest.fixture(scope="class")
    def init_dev_list(self, test_dev_name, log_parser):
        # setup the dev_list collection
        print("\nInserting a device list for test.")
        my_devs = [{'hid': log_parser.home_id,
                    "dev_name": test_dev_name,
                    "ip": "192.168.1.20",
                    "last_updated": datetime.utcfromtimestamp(1397437705)},
                   {'hid': log_parser.home_id,
                    "dev_name": "idea-PC",
                    "ip": "192.168.1.28",
                    "last_updated": datetime.utcfromtimestamp(1397433316)}]

        log_parser.dev_list.insert(my_devs)

    def test_unknown_dev(self, log_parser, access_record, init_dev_list):
        dev_name = log_parser.find_dev_name(access_record['ip'], access_record['accessed_on'])
        assert dev_name is None

    def test_single_dev(self, log_parser, access_record, test_dev_name, init_dev_list):
        """ If there is a single record matching the given IP with an earlier timestamp in the database,
        then get the dev_name from that record.
        """
        new_dev = {'hid': log_parser.home_id,
                   "dev_name": test_dev_name,
                   "ip": "192.168.1.20",
                   "last_updated": datetime.utcfromtimestamp(1397433316)}
        log_parser.dev_list.insert(new_dev)
        dev_name = log_parser.find_dev_name(access_record['ip'], access_record['accessed_on'])
        assert dev_name == test_dev_name

    def test_multi_dev(self, log_parser, access_record, init_dev_list):
        """ If there are more than one record matching the given IP with an earlier timestamp in the database,
        then get the dev_name from the latest record.
        """
        new_dev = {'hid': log_parser.home_id,
                   "dev_name": 'idea-PC',
                   "ip": "192.168.1.20",
                   "last_updated": datetime.utcfromtimestamp(1397435316)}
        log_parser.dev_list.insert(new_dev)
        dev_name = log_parser.find_dev_name(access_record['ip'], access_record['accessed_on'])
        assert dev_name == 'idea-PC'


class TestWriteAccessLog():

    @pytest.fixture(scope="function")
    def reset_collections(self, log_parser):
        log_parser.dev_list.drop()
        log_parser.access_log.drop()
        log_parser.prop.drop()

    def test_process_delayed_access_records(self, log_parser, reset_collections):
        # Prepare test data
        access_records = [{'hid': log_parser.home_id,
                           "dev_name": "idea-PC",
                           "domain": "www.google.com",
                           "accessed_on": datetime.utcfromtimestamp(1397433316)},
                          {'hid': log_parser.home_id,
                           "dev_name": "idea-PC",
                           "domain": "www.facebook.com",
                           "accessed_on": datetime.utcfromtimestamp(1397433316)},
                          {'hid': log_parser.home_id,
                           "ip": "192.168.1.20",
                           "domain": "www.twitter.com",
                           "accessed_on": datetime.utcfromtimestamp(1397437705)},
                          {'hid': log_parser.home_id,
                           "ip": "192.168.1.20",
                           "domain": "www.twitter.com",
                           "accessed_on": datetime.utcfromtimestamp(1397437710)},
                          {'hid': log_parser.home_id,
                           "ip": "192.168.1.20",
                           "domain": "www.twitter.com",
                           "accessed_on": datetime.utcfromtimestamp(1397437805)}]

        dev_list = [{'hid': log_parser.home_id,
                     "dev_name": 'iPhone-5S',
                     "ip": "192.168.1.20",
                     "last_updated": datetime.utcfromtimestamp(1397437705)},
                    {'hid': log_parser.home_id,
                     "dev_name": "idea-PC",
                     "ip": "192.168.1.28",
                     "last_updated": datetime.utcfromtimestamp(1397433316)}]

        latest_dev_list_time = datetime.utcfromtimestamp(1397437750)
        log_parser.dev_list.insert(dev_list)
        log_parser.access_log.insert(access_records)
        log_parser.update_dev_list_latest_updated(latest_dev_list_time)

        log_parser.process_delayed_access_records()
        r1 = log_parser.access_log.find_one({'hid': log_parser.home_id, "accessed_on": datetime.utcfromtimestamp(1397437705)})
        r2 = log_parser.access_log.find_one({'hid': log_parser.home_id, "accessed_on": datetime.utcfromtimestamp(1397437710)})
        r3 = log_parser.access_log.find_one({'hid': log_parser.home_id, "accessed_on": datetime.utcfromtimestamp(1397437805)})
        assert r1['dev_name'] == 'iPhone-5S' and r2['dev_name'] == 'iPhone-5S' \
            and 'ip' not in r1 and 'ip' not in r2 \
            and 'dev_name' not in r3 and r3['ip'] == "192.168.1.20"

    def test_write_access_log(self, log_parser, reset_collections):
        """This test expects:
        1) Only the first 9 lines in the log file are new and never existed in the database.
        2) The first 4 lines have timestamps later than the last time the dev_list was updated.
        """
        access_log = log_parser.access_log
        # Prepare test data
        dev_list = [{'hid': log_parser.home_id, 
                     "dev_name": 'MacBook-Air',
                     "ip": "192.168.1.40",
                     "last_updated": datetime.utcfromtimestamp(1397415116)},
                    {'hid': log_parser.home_id,
                     "dev_name": "iPhone-5S",
                     "ip": "192.168.1.23",
                     "last_updated": datetime.utcfromtimestamp(1397415116)},
                    {'hid': log_parser.home_id,
                     "dev_name": "idea-PC",
                     "ip": "192.168.1.49",
                     "last_updated": datetime.utcfromtimestamp(1397415116)}]
        log_parser.dev_list.insert(dev_list)

        access_records = [
            {'hid': log_parser.home_id,
             "dev_name": "MacBook-Air",
             "domain": "hmma.baidu.com",
             "accessed_on": datetime.utcfromtimestamp(1397415483)},
            {'hid': log_parser.home_id,
             "dev_name": "MacBook-Air",
             "domain": "www.google.com",
             "accessed_on": datetime.utcfromtimestamp(1397415483)}
        ]
        access_log.insert(access_records)

        latest_dev_list_time = datetime.utcfromtimestamp(1397415499)
        log_parser.update_dev_list_latest_updated(latest_dev_list_time)

        # Call the function being tested
        with open('testdata/webmon_recent_domains_test') as f:
            log_parser.write_access_log(f)

        records_ct = access_log.find({'hid': log_parser.home_id}).count()
        records_delay_ct = access_log.find(
            {'hid': log_parser.home_id,
             'dev_name': {'$exists': False},
             'ip': {'$exists': True}}
        ).count()

        assert records_ct == 11 and records_delay_ct == 4


# def test_archive_log():
#     filename = "test_archive_log.txt"
#     gzip_filename = 'archived/'+filename+'.gz'
#     f_in = open(filename, 'w')
#     f_in.write("abcdefg\nhijklmn")
#     f_in.close()
#     f_in = open(filename)
#     log_parser.archive_log(f_in, filename)
#     os.remove(filename)
#     assert os.path.isfile(gzip_filename) and (not os.path.isfile(filename))
#     os.remove(gzip_filename)
