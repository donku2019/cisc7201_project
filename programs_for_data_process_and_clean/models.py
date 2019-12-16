from peewee import *

db = SqliteDatabase(None)

# models
class BaseModel(Model):
    class Meta:
        database = db

class Parish(BaseModel):
    name_cht = TextField(unique=True)
    name_por = TextField()

class ParishPoint(BaseModel):
    parish = ForeignKeyField(Parish, backref = 'points')
    component = IntegerField()
    seq = IntegerField()
    loc_lat = FloatField()
    loc_lon = FloatField()

    class Meta:
        indexes = (
                (('parish', 'component', 'seq'), True),
            )

class StatsZone(BaseModel):
    name_cht = TextField(unique=True)
    name_por = TextField()

class StatsZonePoint(BaseModel):
    stats_zone = ForeignKeyField(StatsZone, backref = 'points')
    component = IntegerField()
    seq = IntegerField()
    loc_lat = FloatField()
    loc_lon = FloatField()

    class Meta:
        indexes = (
                (('stats_zone', 'component', 'seq'), True),
            )

class BusStop(BaseModel):
    year = IntegerField()
    month = IntegerField()
    stop_code = TextField() 
    parent_code = TextField()
    name_cht = TextField()
    name_por = TextField()
    loc_lat = FloatField()
    loc_lon = FloatField()
    parish = ForeignKeyField(Parish, backref = 'bus_stops', null = True)
    stats_zone = ForeignKeyField(StatsZone, backref = 'bus_stops', null = True)

    class Meta:
        indexes = (
                (('year', 'month', 'stop_code'), True),
            )

class BusAgency(BaseModel):
    year = IntegerField()
    month = IntegerField()
    name_cht = TextField()
    name_por = TextField()
    phone = TextField()
    website = TextField()

    class Meta:
        indexes = (
                (('year', 'month', 'name_cht'), True),
            )

class BusRoute(BaseModel):
    year = IntegerField()
    month = IntegerField()
    agency = ForeignKeyField(BusAgency, backref = 'routes')
    route_code = TextField()
    name_cht = TextField()
    name_por = TextField()
    route_type = TextField()

    class Meta:
        indexes = (
                (('year', 'month', 'route_code'), True),
            )

class BusSubRoute(BaseModel):
    year = IntegerField()
    month = IntegerField()
    route = ForeignKeyField(BusRoute, backref = 'sub_routes')
    sub_route_code = TextField()
    back_forth = IntegerField()
    name_cht = TextField()
    name_por = TextField()

    class Meta:
        indexes = (
                (('year', 'month', 'sub_route_code', 'back_forth'), True),
            )

class SubRouteStop(BaseModel):
    year = IntegerField()
    month = IntegerField()
    sub_route = ForeignKeyField(BusSubRoute, backref = 'stops')
    stop_seq = IntegerField()
    bus_stop = ForeignKeyField(BusStop, backref = 'sub_routes')

    class Meta:
        indexes = (
                (('year', 'month', 'sub_route', 'stop_seq'), True),
            )

class SubRouteBusStopRaw(BaseModel):
    year = IntegerField()
    month = IntegerField()
    raw_stop_code = TextField()
    loc_lat = FloatField()
    loc_lon = FloatField()
    parish = ForeignKeyField(Parish, backref = 'sub_route_points', null = True)
    stats_zone = ForeignKeyField(StatsZone, backref = 'sub_route_points', null = True)

    class Meta:
        indexes = (
                (('year', 'month', 'raw_stop_code'), True),
            )

class SubRoutePoint(BaseModel):
    year = IntegerField()
    month = IntegerField()
    sub_route = ForeignKeyField(BusSubRoute, backref = 'points')
    seq = IntegerField()
    loc_lat = FloatField()
    loc_lon = FloatField()
    sub_route_stop = ForeignKeyField(SubRouteStop, backref = 'sub_routes', null = True)
    raw_stop_code = ForeignKeyField(SubRouteBusStopRaw, backref = 'sub_routes_points', null = True)
    parish = ForeignKeyField(Parish, backref = 'sub_route_points', null = True)
    stats_zone = ForeignKeyField(StatsZone, backref = 'sub_route_points', null = True)

    class Meta:
        indexes = (
                (('year', 'month', 'sub_route', 'seq'), True),
            )

class SubRouteDistance(BaseModel):
    year = IntegerField()
    month = IntegerField()
    sub_route = ForeignKeyField(BusSubRoute, backref = 'distances')
    seq = IntegerField()
    from_stop = ForeignKeyField(SubRouteBusStopRaw, backref = 'distance_from_stops', null = True)
    to_stop = ForeignKeyField(SubRouteBusStopRaw, backref = 'distance_to_stops', null = True)
    distance = FloatField()

    class Meta:
        indexes = (
                (('year', 'month', 'sub_route', 'seq'), True),
            )

class SubRouteSchedule(BaseModel):
    year = IntegerField()
    month = IntegerField()
    sub_route = ForeignKeyField(BusSubRoute, backref = 'schedules')
    arrival_time = TimeField()
    day_type = TextField()

    class Meta:
        indexes = (
                (('year', 'month', 'sub_route', 'arrival_time', 'day_type'), True),
            )

# methods
def init_db(filename):
    db.init(filename, pragmas={
        # 'journal_mode': 'wal',
        # 'cache_size': -1 * 64000,  # 64MB
        'foreign_keys': 1,
        'ignore_check_constraints': 0,
        'synchronous': 0,
    })

def create_tables():
    with db:
        db.create_tables([
            Parish,
            ParishPoint,
            StatsZone,
            StatsZonePoint,
            BusStop,
            BusAgency,
            BusRoute,
            BusSubRoute,
            SubRouteStop,
            SubRouteBusStopRaw,
            SubRoutePoint,
            SubRouteDistance,
            SubRouteSchedule
        ])
