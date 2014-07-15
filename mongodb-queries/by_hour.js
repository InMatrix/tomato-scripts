// mongo 192.168.1.6/router_logs --quiet my_query.js > output.json
rs = db.access_log.aggregate({	$sort: {accessed_on: 1}}, 
						{	$group: 
							{ _id: { year: {$year: '$accessed_on'}, month: {$month: '$accessed_on'}, day: {$dayOfMonth: '$accessed_on'}, hour: {$hour: '$accessed_on'}}, 
		  					count: { $sum: 1} } },
						{	$sort: {_id: 1}});

output = [];
timezone_offset = -14400000
rs.result.forEach(function(r){
	var utc_time = Date.UTC(r._id.year, r._id.month-1, r._id.day, r._id.hour)
	output.push([ utc_time + timezone_offset, r.count ])
});

printjson(output);
