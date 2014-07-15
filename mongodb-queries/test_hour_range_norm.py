from datetime import datetime
from delorean import Delorean

import hour_range_norm as hrn

def test_compute_time_diff():
	t = Delorean(datetime(2014, 6, 5, 13), timezone='UTC')
	start_time = Delorean(datetime(2014, 6, 5, 7), timezone='US/Eastern')
	t.shift('US/Eastern')
	hour_diff = hrn.compute_hour_diff(start_time.datetime, t.datetime)
	assert hour_diff == 2

def test_normalize():
	raw = [1, 2, 3, 4, 5, 0]
	transformed = hrn.normalize(raw)
	assert cmp(transformed, [(1-3)/3.0, (2-3)/3.0, (3-3)/3.0, (4-3)/3.0, (5-3)/3.0, (0-3)/3.0]) == 0