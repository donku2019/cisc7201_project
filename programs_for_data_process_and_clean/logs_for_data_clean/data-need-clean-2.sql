-- get bus stops that without any route
SELECT
	BS.*,
	subRS.route_count
FROM
	busstop BS
	left outer join (select bus_stop_id, count(*) route_count from subroutestop group by bus_stop_id) subRS on BS.id = subRS.bus_stop_id
WHERE
	subRS.route_count is null;

-- get routes that without any bus stop
SELECT
	subR.*,
	subRS.stop_count
FROM
	bussubroute subR
	left outer join (select sub_route_id, count(*) stop_count from subroutestop group by sub_route_id) subRS on subR.id = subRS.sub_route_id
WHERE
	subRS.stop_count is null;

