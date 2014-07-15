/*
 * mongo 192.168.1.6/router_logs --quiet by_device_hour.js > device_hour.json
 */


var rs = db.access_log.aggregate(
						{	$group: 
							{ _id: {
									dev_name: '$dev_name', 
									time:{ year: {$year: '$accessed_on'}, month: {$month: '$accessed_on'}, day: {$dayOfMonth: '$accessed_on'}, hour: {$hour: '$accessed_on'}} 
		  							},
		  					count: { $sum: 1} } },
						{	$sort: {'_id.time': 1}});

var utc_time, r_time
var output = {};
var timezone_offset = -14400000
rs.result.forEach(function(r){
	r_time = r._id.time
	utc_time = Date.UTC(r_time.year, r_time.month-1, r_time.day, r_time.hour)
	if (!(r._id.dev_name in output)){
		// if the device name has yet existed in the output object, then add it with an empty array to be filled.
		output[r._id.dev_name] = []
	}
	output[r._id.dev_name].push([ utc_time + timezone_offset, r.count ])
});

printjson(output)