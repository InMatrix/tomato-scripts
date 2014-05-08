def test_parse_webmon():
	error_num = 0
	with open('testdata/webmon_recent_domains_test') as f:
		for line in f:
			record = line.split()
			if len(record) != 3:
				error_num += 1
	assert error_num == 0
