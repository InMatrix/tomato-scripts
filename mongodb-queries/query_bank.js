
// Aggregate by hour
// mongo 192.168.1.6/router_logs query.js > output.json
rs = db.access_log.aggregate(
	{$group: 
		{ _id: 
			{ 
				day: {$dayOfMonth: "$accessed_on"}, 
				hour: {$hour: "$accessed_on"}, 
			}, 
			count:{ $sum: 1} 
		}
	});

printjson(rs)


// include each group's first timestamp
db.access_log.aggregate({	$sort: {accessed_on: 1}}, 
						{	$group: 
							{ _id: { day: {$dayOfMonth: '$accessed_on'}, hour: {$hour: '$accessed_on'}},
		  					count: { $sum: 1}, first: { $first: '$accessed_on'} } },
						{	$sort: {_id: 1}});

// include each group's first timestamp, and change the structure of document
db.access_log.aggregate({	$sort: {accessed_on: 1}}, 
						{	$group: 
							{ _id: { day: {$dayOfMonth: '$accessed_on'}, hour: {$hour: '$accessed_on'}}, 
		  					count: { $sum: 1}, first: { $first: '$accessed_on'} } },
						{	$sort: {_id: 1}},
						{	$project: {
								_id: 0,
								first:1,
								t: 1,
						}});

// add a time offset.
db.access_log.aggregate({	$sort: {accessed_on: 1}}, 
						{	$group: 
							{ _id: { day: {$dayOfMonth: '$accessed_on'}, hour: {$hour: '$accessed_on'}}, 
		  					count: { $sum: 1}, first: { $first: '$accessed_on'} } },
						{	$sort: {_id: 1}},
						{	$project: {
								_id: 0,
								first:{$add:["$first",1000]},
								t: 1,
						}});


db.access_log.aggregate({	$sort: {accessed_on: 1}}, 
						{	$group: 
							{ _id: { year: {$year: '$accessed_on'}, month: {$month: '$accessed_on'}, day: {$dayOfMonth: '$accessed_on'}, hour: {$hour: '$accessed_on'}}, 
		  					count: { $sum: 1} } },
						{	$sort: {_id: 1}});