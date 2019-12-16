import os
import re
import json
import copy
import models
import pandas as pd
import numpy as np
from datetime import datetime
from models import *
from peewee import *
from shapely.geometry import Point, Polygon
from math import sin, cos, sqrt, atan2, radians

rootDir = ''
logFilename = ''

def is_json(filename):
    return re.match(r'.+\.json', filename)

def is_hidden_file(filename):
    return re.match(r'\..+', filename)

def get_or_insert_parish(parishJson):
    try:
        parishRec = Parish.select().where(
            Parish.name_cht == parishJson['nameCht']
        ).get()
    except DoesNotExist:
        parishRec = Parish(
            name_cht = parishJson['nameCht'],
            name_por = parishJson['namePor']
        )
        parishRec.save()

    return parishRec

def insert_parish(folderName):
    if os.path.exists(folderName):
        os.chdir(folderName)

        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        for f in files:
            if not is_json(f):
                continue
            if is_hidden_file(f):
                continue

            print('>> processing {folder}'.format(folder = f), end = '\r')
            with open(f, 'r', encoding="utf-8") as parishFile:
                parishJson = json.load(parishFile)

                for parish in parishJson['features']:
                    parishProps = parish['properties']
                    # for sqlite parish
                    parishRec = get_or_insert_parish(parishProps)

                    parishPointMatrix = pd.DataFrame(columns = ['parish_id', 'component', 'seq', 'loc_lat', 'loc_lon'])
                    for compIndex, parishComp in enumerate(parish['geometry']['coordinates']):
                        for pointSeq, parishCompPoint in enumerate(parishComp):
                            parishPointMatrix = parishPointMatrix.append({
                                'parish_id': parishRec.id,
                                'component': compIndex,
                                'seq': pointSeq,
                                'loc_lat': parishCompPoint[1],
                                'loc_lon': parishCompPoint[0]
                            }, ignore_index = True)
                    
                    data = list(parishPointMatrix.T.to_dict().values())
                    ParishPoint.insert_many(data).execute()

def get_or_insert_stats_zone(statsZoneJson):
    try:
        statsZoneRec = StatsZone.select().where(StatsZone.name_cht == statsZoneJson['nameCht']).get()
    except DoesNotExist:
        statsZoneRec = StatsZone(
            name_cht = statsZoneJson['nameCht'],
            name_por = statsZoneJson['namePor']
        )
        statsZoneRec.save()

    return statsZoneRec

def insert_stats_zone(folderName):
    if os.path.exists(folderName):
        os.chdir(folderName)

        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        for f in files:
            if not is_json(f):
                continue

            print('>> processing {folder}'.format(folder = f), end = '\r')
            with open(f, 'r', encoding="utf-8") as statsZoneFile:
                statsZoneJson = json.load(statsZoneFile)

                for statsZone in statsZoneJson['features']:
                    statsZoneProps = statsZone['properties']
                    # for sqlite statsZone
                    statsZoneRec = get_or_insert_stats_zone(statsZoneProps)

                    statsZoneMatrix = pd.DataFrame(columns = ['stats_zone_id', 'component', 'seq', 'loc_lat', 'loc_lon'])
                    for compIndex, statsZoneComp in enumerate(statsZone['geometry']['coordinates']):
                        for pointSeq, statsZoneCompPoint in enumerate(statsZoneComp):
                            statsZoneMatrix = statsZoneMatrix.append({
                                'stats_zone_id': statsZoneRec.id,
                                'component': compIndex,
                                'seq': pointSeq,
                                'loc_lat': statsZoneCompPoint[1],
                                'loc_lon': statsZoneCompPoint[0]
                            }, ignore_index = True)

                    data = list(statsZoneMatrix.T.to_dict().values())
                    StatsZonePoint.insert_many(data).execute()

def get_or_insert_bus_stop(year, month, busStop):
    try:
        busStopRec = BusStop.select().where(
            (BusStop.year == year) &
            (BusStop.month == month) &
            (BusStop.stop_code == busStop['code'])
        ).get()
    except DoesNotExist:
        busStopRec = BusStop(    
            year = year,
            month = month,
            stop_code = busStop['code'],
            parent_code = busStop['parentCode'] if 'parentCode' in busStop else busStop['code'],
            name_cht = busStop['nameCht'],
            name_por = busStop['namePor'],
            loc_lat = busStop['lat'],
            loc_lon = busStop['lng']
        )
        busStopRec.save()

    return busStopRec

def get_bus_stop(year, month, busStop):
    originBusStopCode = copy.copy(busStop['code'])
    count = 0
    busStopRec = None

    busStop['code'] = re.sub(r'/(.+)', '', busStop['code'])
    busStop['code'] = re.sub(r'/(.+)', '', busStop['code'])

    while (busStopRec == None) and count < 3:
        try:
            busStopRec = BusStop.select().where(
                (BusStop.year == year) &
                (BusStop.month == month) &
                (BusStop.stop_code == busStop['code'])
            ).get()
        except DoesNotExist:
            busStopRec = None

        if busStopRec == None:
            try:
                busStopRec = BusStop.select().where(
                    (BusStop.year == year) &
                    (BusStop.month == month) &
                    (BusStop.parent_code == busStop['code'])
                ).get()
            except DoesNotExist:
                busStopRec = None

        if busStopRec == None:
            busStop['code'] = re.sub(r'\_[^_]+$', '', busStop['code'])

        count += 1

    if busStopRec == None:
        try:
            busStopCode = re.findall(r'([^\_]+\_).+', busStop['code'])
            busStopCode = busStopCode if len(busStopCode) > 0 else busStop['code'] + '_'

            busStopRec = BusStop.select().where(
                (BusStop.year == year) &
                (BusStop.month == month) & (
                    (BusStop.stop_code.startswith(busStopCode)) |
                    (BusStop.parent_code.startswith(busStopCode))
                )
            ).get()
        except DoesNotExist:
            busStopRec = None

    if (not busStopRec == None) and (not originBusStopCode == busStopRec.stop_code):
        return originBusStopCode, busStopRec

    return None, busStopRec

def insert_bus_stops(folderName):
    if os.path.exists(folderName):
        os.chdir(folderName)

        files = [f for f in os.listdir('.') if os.path.isdir(f)]
        files.sort()
        for f in files:
            print('>> processing {folder}'.format(folder = f), end = '\r')
            yearMonth = re.findall(r'(\d{4})_(\d{2})', f)[0]
            year = int(yearMonth[0])
            month = int(yearMonth[1])

            with open('{month_folder}/{filename}'.format(month_folder = f, filename = 'stops.json'), 'r', encoding="utf-8") as busStopFile:
                busStopJson = json.load(busStopFile)

                for busStop in busStopJson:
                    busStopRec = get_or_insert_bus_stop(year, month, busStop)

def get_or_insert_bus_agency(year, month, busAgency):
    try:
        busAgencyRec = BusAgency.select().where(
            (BusAgency.year == year) &
            (BusAgency.month == month) &
            (BusAgency.name_cht == busAgency['nameCht'])
        ).get()
    except DoesNotExist:
        busAgencyRec = BusAgency(
            year = year,
            month = month,
            name_cht = busAgency['nameCht'],
            name_por = busAgency['namePor'],
            phone = busAgency['phone'],
            website = busAgency['website']
        )
        busAgencyRec.save()

    return busAgencyRec

def get_or_insert_bus_route(year, month, busRoute, busAgencyRec):
    try:
        busRouteRec = BusRoute.select().where(
            (BusRoute.year == year) &
            (BusRoute.month == month) &
            (BusRoute.route_code == busRoute['code'])
        ).get()
    except DoesNotExist:
        busRouteRec = BusRoute(
            year = year,
            month = month,
            agency = busAgencyRec,
            route_code = busRoute['code'],
            name_cht = busRoute['nameCht'],
            name_por = busRoute['namePor'],
            route_type = busRoute['type'] if 'type' in busRoute else ('2-way' if len(busRoute['directions']) > 1 else 'circular')
        )
        busRouteRec.save()

    return busRouteRec

def get_bus_route(year, month, busRoute, busAgencyRec):
    try:
        busRouteRec = BusRoute.select().where(
            (BusRoute.year == year) &
            (BusRoute.month == month) &
            (BusRoute.route_code == busRoute['code'])
        ).get()
    except DoesNotExist:
        busRouteRec = None

    return busRouteRec

def get_or_insert_bus_sub_route(year, month, busSubRoute, back_forth, busRouteRec):
    try:
        busSubRouteRec = BusSubRoute.select().where(
            (BusSubRoute.year == year) &
            (BusSubRoute.month == month) &
            (BusSubRoute.sub_route_code == busSubRoute['code']) &
            (BusSubRoute.back_forth == back_forth)
        ).get()
    except DoesNotExist:
        busSubRouteRec = BusSubRoute(
            year = year,
            month = month,
            route = busRouteRec,
            sub_route_code = busSubRoute['code'],
            back_forth = back_forth,
            name_cht = busSubRoute['nameCht'],
            name_por = busSubRoute['namePor']
        )
        busSubRouteRec.save()

    return busSubRouteRec

def get_bus_sub_route(year, month, busSubRoute, back_forth, busRouteRec):
    try:
        busSubRouteRec = BusSubRoute.select().where(
            (BusSubRoute.year == year) &
            (BusSubRoute.month == month) &
            (BusSubRoute.sub_route_code == busSubRoute['code']) &
            (BusSubRoute.back_forth == back_forth)
        ).get()
    except DoesNotExist:
        busSubRouteRec = None

    return busSubRouteRec

def get_or_insert_bus_sub_route_stop(year, month, subRouteStop, stopSeq, busSubRouteRec):
    originBusStopCode, busStopRec = get_bus_stop(year, month, subRouteStop)

    if busStopRec == None:
        append_log('Sub Route Stop not found: {year},{month},{route},{back_forth},{stop}'.format(route = busSubRouteRec.sub_route_code, year = year, month = month, back_forth = busSubRouteRec.back_forth, stop = subRouteStop['code']))
        return None

    if not originBusStopCode == None:
        append_log('Stop code revised for finding bus stop: {year},{month},{route},{back_forth},{stop} to {revised_stop}'.format(route = busSubRouteRec.sub_route_code, year = year, month = month, back_forth = busSubRouteRec.back_forth, stop = originBusStopCode, revised_stop = busStopRec.stop_code))

    try:
        subRouteStopRec = SubRouteStop.select().where(
            (SubRouteStop.year == year) &
            (SubRouteStop.month == month) &
            (SubRouteStop.sub_route == busSubRouteRec.id) &
            (SubRouteStop.stop_seq == stopSeq)
        ).get()
    except:
        subRouteStopRec = SubRouteStop(
            year = year,
            month = month,
            sub_route = busSubRouteRec,
            stop_seq = stopSeq,
            bus_stop = busStopRec
        )
        subRouteStopRec.save()

    return subRouteStopRec

def get_or_insert_bus_sub_route_stop_raw(year, month, subRouteStop, point):
    try:
        subRouteStopRawRec = SubRouteBusStopRaw.select().where(
            (SubRouteBusStopRaw.year == year) &
            (SubRouteBusStopRaw.month == month) &
            (SubRouteBusStopRaw.raw_stop_code == subRouteStop['code'])
        ).get()
    except:
        subRouteStopRawRec = SubRouteBusStopRaw(
            year = year,
            month = month,
            raw_stop_code = subRouteStop['code'],
            loc_lat = point['lat'],
            loc_lon = point['lon']
        )
        subRouteStopRawRec.save()

    return subRouteStopRawRec

def insert_bus_routes(folderName):
    if os.path.exists(folderName):
        os.chdir(folderName)

        files = [f for f in os.listdir('.') if os.path.isdir(f)]
        files.sort()
        for f in files:
            print('>> processing {folder}'.format(folder = f), end = '\r')
            yearMonth = re.findall(r'(\d{4})_(\d{2})', f)[0]
            year = int(yearMonth[0])
            month = int(yearMonth[1])

            with open('{month_folder}/{filename}'.format(month_folder = f, filename = 'routes.json'), 'r', encoding="utf-8") as busStopFile:
                busRouteJson = json.load(busStopFile)

                for busRoute in busRouteJson:
                    # agency
                    if 'agency' in busRoute:
                        busAgencyRec = get_or_insert_bus_agency(year, month, busRoute['agency'])
                    elif 'agencies' in busRoute:
                        busAgencyRec = get_or_insert_bus_agency(year, month, busRoute['agencies'][0])

                    # route
                    busRouteRec = get_or_insert_bus_route(year, month, busRoute, busAgencyRec)

                    # sub route
                    for back_forth, busSubRoute in enumerate(busRoute['directions']):
                        busSubRouteRec = get_or_insert_bus_sub_route(year, month, busSubRoute, back_forth, busRouteRec)

def get_bus_sub_route_point(year, month, subRouteRec, point):
    try:
        subRoutePointRec = SubRoutePoint.select().where(
            (SubRoutePoint.year == year) &
            (SubRoutePoint.month == month) &
            (SubRoutePoint.sub_route_id == subRouteRec.id) &
            (SubRoutePoint.loc_lat == point['lat']) &
            (SubRoutePoint.loc_lon == point['lon']) &
            (SubRoutePoint.sub_route_stop_id.is_null(True))
        ).get()
    except DoesNotExist:
        subRoutePointRec = None

    return subRoutePointRec

def get_parish_poly_list():
    l = []
    parishes = Parish.select()

    for parish in parishes:
        polyPoints = parish.points.order_by(ParishPoint.seq)
        l.append({'parish_id': parish.id, 'poly': Polygon([(a.loc_lat, a.loc_lon) for a in polyPoints])})

    return l

def get_parish_id_of_point(point, polyList):
    p = Point(point['lat'], point['lon'])

    for poly in polyList:
        if p.within(poly['poly']):
            return poly['parish_id']

    return None

def insert_bus_route_points(folderName, fix_data = False):
    if os.path.exists(folderName):
        os.chdir(folderName)
        color_back_forth_mapping = {
            '#FFC853' : 0,
            '#7F642A' : 1
        }
        parishPolyList = get_parish_poly_list()
        if not fix_data:
            SubRoutePoint.delete().execute()
            SubRouteStop.delete().execute()

        files = [f for f in os.listdir('.') if os.path.isdir(f)]
        files.sort()
        for f in files:
            # if not str(f) == '2016_02':
            #   continue

            print('>> processing {folder}'.format(folder = f))
            yearMonth = re.findall(r'(\d{4})_(\d{2})', f)[0]
            year = int(yearMonth[0])
            month = int(yearMonth[1])

            monthFolder = './{month_folder}/geojson'.format(month_folder = f)
            routeFiles = [f2 for f2 in os.listdir(monthFolder) if os.path.isfile(monthFolder + '/' + f2)]
            for routeFileName in routeFiles:
                if not is_json(routeFileName):
                    continue

                routeCode = re.findall(r'(.+)\.json', routeFileName)[0]
                print('>> processing {year}_{month}_{route}'.format(year = year, month = month, route = routeCode), end = '\r')

                # if not routeCode == '1':
                #   continue

                busRouteRec = get_bus_route(year, month, {'code': routeCode}, None)
                if busRouteRec == None:
                    append_log('Route: {year},{month},{route}'.format(route =routeCode, year = year, month = month))
                    continue

                with open(monthFolder + '/' + routeFileName, 'r', encoding="utf-8") as routeFile:
                    routeJson = json.load(routeFile)
                    lineJson = [r for r in routeJson['features'] if r['geometry']['type'] == 'LineString']
                    # every point of route
                    if not fix_data:
                        for back_forth, line in enumerate(lineJson):
                            subRouteRec = get_bus_sub_route(year, month, {'code': busRouteRec.route_code}, back_forth, busRouteRec)
                            if subRouteRec == None:
                                append_log('Sub Route: {year},{month},{route},{back_forth}'.format(route =routeCode, year = year, month = month, back_forth = back_forth))
                                continue

                            routePointMatrix = pd.DataFrame(columns = ['year', 'month', 'sub_route_id', 'seq', 'loc_lat', 'loc_lon', 'parish_id'])
                            for seq, point in enumerate(line['geometry']['coordinates']):
                                routePointMatrix = routePointMatrix.append({
                                    'year': year, 
                                    'month': month,
                                    'sub_route_id': subRouteRec.id,
                                    'seq': seq,
                                    'loc_lat': point[1],
                                    'loc_lon': point[0],
                                    'parish_id': get_parish_id_of_point({'lat':point[1], 'lon':point[0]}, parishPolyList)
                                }, ignore_index = True)

                            SubRoutePoint.insert_many(routePointMatrix.T.to_dict().values()).execute()

                    # check is a bus stop point or not
                    stopSeq = 1
                    busStopJson = [r for r in routeJson['features'] if r['geometry']['type'] == 'Point']
                    for point in busStopJson:
                        back_forth = color_back_forth_mapping[point['properties']['marker-color']]
                        pointCoor = {'lat': point['geometry']['coordinates'][1], 'lon': point['geometry']['coordinates'][0]}
                        subRouteRec = get_bus_sub_route(year, month, {'code': busRouteRec.route_code}, back_forth, busRouteRec)
                        if subRouteRec == None:
                            append_log('Sub Route: {year},{month},{route},{back_forth}'.format(route =routeCode, year = year, month = month, back_forth = back_forth))
                            continue
                        stopCode = point['properties']['stopCode']

                        print('>> processing: {year}_{month}_{route}_{stop}'.format(year = year, month = month, route = subRouteRec.sub_route_code, stop = stopCode), end='\r')
                        # sub route stops
                        subRouteStopRec = get_or_insert_bus_sub_route_stop(year, month, {'code': stopCode}, stopSeq, subRouteRec)
                        stopSeq += 1
                        subRoutePointRec = get_bus_sub_route_point(year, month, subRouteRec, pointCoor)
                        subRouteStopRawRec = get_or_insert_bus_sub_route_stop_raw(year, month, {'code': stopCode}, pointCoor)

                        if subRoutePointRec == None:
                            append_log('Sub Route Point: {year},{month},{route},{back_forth},{lat},{lon}'.format(route =routeCode, year = year, month = month, back_forth = back_forth, lat = pointCoor['lat'], lon = pointCoor['lon']))
                            continue
                        
                        if subRouteStopRec == None:
                            append_log('Sub Route Stop: {year},{month},{route},{stop}'.format(route =routeCode, year = year, month = month, stop = stopCode))
                            continue

                        if subRouteStopRawRec == None:
                            append_log('Sub Route Stop Raw: {year},{month},{route},{stop}'.format(route =routeCode, year = year, month = month, stop = stopCode))
                            continue

                        subRoutePointRec.sub_route_stop = subRouteStopRec
                        subRoutePointRec.raw_stop_code = subRouteStopRawRec
                        subRoutePointRec.save()

def get_geo_distance(point1, point2):
    R = 6373.0

    lat1 = radians(point1.loc_lat)
    lon1 = radians(point1.loc_lon)
    lat2 = radians(point2.loc_lat)
    lon2 = radians(point2.loc_lon)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

def calculate_distance():
    SubRouteDistance.delete().execute()

    subRouteRecs = BusSubRoute.select().order_by(
        BusSubRoute.year,
        BusSubRoute.month,
        BusSubRoute.route_id,
        BusSubRoute.back_forth
    )

    for subRouteRec in subRouteRecs:
        print('>> processing {year}_{month}_{route_code}_{back_forth}'.format(
                year = subRouteRec.year,
                month = subRouteRec.month,
                route_code = subRouteRec.sub_route_code,
                back_forth = subRouteRec.back_forth
            ), end = '\r')
        fail = False

        distance = 0
        distanceRec = {}
        distanceList = []
        stopPoint = None
        point1 = None
        point2 = None
        disSeq = 1
        lastPointIndex = len(subRouteRec.points) - 1
        for pointIndex, subRoutePointRec in enumerate(subRouteRec.points):
            point2 = point1
            point1 = subRoutePointRec
            if pointIndex == 0:
                if point1.raw_stop_code == None:
                    append_log('First Point on Sub Route: {year},{month},{route},{back_forth}'.format(route =subRouteRec.sub_route_code, year = subRouteRec.year, month = subRouteRec.month, back_forth = subRouteRec.back_forth))
                    fail = True
                    break
                stopPoint = point1
                continue
            distance += get_geo_distance(point1, point2)

            if (not subRoutePointRec.raw_stop_code == None) or (pointIndex == lastPointIndex):
                if subRoutePointRec.raw_stop_code == None:
                    append_log('Last Point on Sub Route: {year},{month},{route},{back_forth}'.format(route =subRouteRec.sub_route_code, year = subRouteRec.year, month = subRouteRec.month, back_forth = subRouteRec.back_forth))
                    fail = True
                    break

                distanceRec = {
                    'year': subRouteRec.year,
                    'month': subRouteRec.month,
                    'sub_route_id': subRouteRec.id,
                    'seq': disSeq,
                    'from_stop_id': stopPoint.raw_stop_code.id,
                    'to_stop_id': subRoutePointRec.raw_stop_code.id,
                    'distance': distance
                }
                distanceList.append(copy.copy(distanceRec))

                distance = 0
                stopPoint = subRoutePointRec
                disSeq += 1

        if fail:
            continue

        SubRouteDistance.insert_many(distanceList).execute()

def nat_check(nat):
    return nat == np.datetime64('NaT')

def clean_min_interval(interval):
    try:
        if re.match(r'(\d+)-(\d+)', interval):
            return round((int(re.findall(r'(\d+)-(\d+)', interval)[0][0]) + int(re.findall(r'(\d+)-(\d+)', interval)[0][1]))/ 2, 0)
        else:
            return int(interval)
    except:
        return 6

def create_bus_schedule(folderName):
    columnIndex = ['route_code', 'back_forth', 'time_fr', 'time_to', 'minute_interval', 'day_type']
    
    SubRouteSchedule.delete().execute()

    if os.path.exists(folderName):
        os.chdir(folderName)

        files = [f for f in os.listdir('.') if os.path.isdir(f)]
        files.sort()
        for f in files:
            print('>> processing {month}'.format(month = f))
            yearMonth = re.findall(r'(\d{4})_(\d{2})', f)[0]
            year = int(yearMonth[0])
            month = int(yearMonth[1])

            # if not str(f) == '2016_02':
            #     continue
            
            monthFolder = './{month_folder}/route-schedule'.format(month_folder = f)
            routeFiles = [f2 for f2 in os.listdir(monthFolder) if os.path.isfile(monthFolder + '/' + f2)]

            for routeFile in routeFiles:
                print('>> processing {route}'.format(route = routeFile), end = '\r')
                # if not (routeFile == '2'):
                #     continue
                schedule_route_matrix = pd.DataFrame(columns = ['year', 'month', 'sub_route_id', 'arrival_time', 'day_type'])
                schedules = pd.read_csv(monthFolder + '/' + routeFile, names = columnIndex)
                schedules['minute_interval'] = schedules['minute_interval'].apply(clean_min_interval)

                sub_routes = schedules.groupby(['route_code', 'back_forth'])
                sub_routes = sub_routes.size().reset_index()

                for index, sub_route in sub_routes.iterrows():
                    schedule_template_matrix = pd.DataFrame(columns = ['year', 'month', 'sub_route_id', 'arrival_time', 'day_type'])
                    schedule_plans = schedules[(schedules.route_code == sub_route.route_code) & (schedules.back_forth == sub_route.back_forth)]

                    for index, schedule_plan in schedule_plans.iterrows():
                        if pd.isnull(schedule_plan.time_fr) or pd.isnull(schedule_plan.time_to):
                            continue
                        schedule = pd.date_range(schedule_plan.time_fr, schedule_plan.time_to, freq=str(schedule_plan.minute_interval)+'min', name='arrival_time').to_frame()
                        schedule['arrival_time'] = schedule['arrival_time'].apply(lambda x: x.time())
                        schedule['year'] = year
                        schedule['month'] = month
                        schedule['sub_route_id'] = None
                        schedule['day_type'] = schedule_plan.day_type
                        schedule_template_matrix = schedule_template_matrix.append(schedule, sort=False)
                    
                    schedule_template_matrix = schedule_template_matrix.reset_index()
                    del schedule_template_matrix['index']

                    subRouteRec = get_bus_sub_route(year, month, {'code': sub_route.route_code}, sub_route.back_forth, None)
                    if subRouteRec == None:
                        append_log('Sub Route: {year},{month},{route},{back_forth}'.format(route =sub_route.route_code, year = year, month = month, back_forth = sub_route.back_forth))
                        continue
                    schedule_template_matrix['sub_route_id'] = subRouteRec.id
                    schedule_template_matrix = schedule_template_matrix.drop_duplicates()

                    schedule_route_matrix = schedule_route_matrix.append(copy.copy(schedule_template_matrix), sort=False)
                
                schedule_route_matrix = schedule_route_matrix.reset_index()
                del schedule_route_matrix['index']
                SubRouteSchedule.insert_many(schedule_route_matrix.T.to_dict().values()).execute()

def parish_bus_stop_mapping():
    parishes = Parish.select()

    for parish in parishes:
        polyPoints = parish.points.order_by(ParishPoint.seq)
        parishPoly = Polygon([(a.loc_lat, a.loc_lon) for a in polyPoints])
        print('>> processing {parish}'.format(parish = parish.name_cht))

        # bus stop
        for busStop in BusStop.select().where(BusStop.parish.is_null(True)):
            print('>> processing busStop{year}_{month}_{bus_stop}'.format(year = busStop.year, month = busStop.month, bus_stop = busStop.stop_code), end = '\r')
            busStopPoint = Point(busStop.loc_lat, busStop.loc_lon)
            if busStopPoint.within(parishPoly):
                busStop.parish = parish
                busStop.save()

        # raw bus stop
        for rawBusStop in SubRouteBusStopRaw.select().where(SubRouteBusStopRaw.parish.is_null(True)):
            print('>> processing rawBusStop{year}_{month}_{bus_stop}'.format(year = rawBusStop.year, month = rawBusStop.month, bus_stop = rawBusStop.raw_stop_code), end = '\r')
            rawBusStopPoint = Point(rawBusStop.loc_lat, rawBusStop.loc_lon)
            if rawBusStopPoint.within(parishPoly):
                rawBusStop.parish = parish
                rawBusStop.save()

def stats_zone_bus_stop_mapping():
    statsZones = StatsZone.select()

    for statsZone in statsZones:
        polyPoints = statsZone.points.order_by(StatsZonePoint.seq)
        statsZonePoly = Polygon([(a.loc_lat, a.loc_lon) for a in polyPoints])
        print('>> processing {folder}'.format(folder = statsZone.name_cht))

        # bus stop
        for busStop in BusStop.select().where(BusStop.stats_zone.is_null(True)):
            print('>> processing busStop{year}_{month}_{bus_stop}'.format(year = busStop.year, month = busStop.month, bus_stop = busStop.stop_code), end = '\r')
            busStopPoint = Point(busStop.loc_lat, busStop.loc_lon)
            if busStopPoint.within(statsZonePoly):
                busStop.stats_zone = statsZone
                busStop.save()

        # raw bus stop
        for rawBusStop in SubRouteBusStopRaw.select().where(SubRouteBusStopRaw.stats_zone.is_null(True)):
            print('>> processing rawBusStop{year}_{month}_{bus_stop}'.format(year = rawBusStop.year, month = rawBusStop.month, bus_stop = rawBusStop.raw_stop_code), end = '\r')
            rawBusStopPoint = Point(rawBusStop.loc_lat, rawBusStop.loc_lon)
            if rawBusStopPoint.within(statsZonePoly):
                rawBusStop.stats_zone = statsZone
                rawBusStop.save()


def append_log(line):
    with open(rootDir + '/' + logFilename + '.txt', 'a', encoding="utf-8") as logFile:
        logFile.write(line + '\n')

if __name__ == '__main__':
    rootDir = os.getcwd()
    now = datetime.now()
    logFilename = '{year}{month}{day}{hour}{minute}{second}'.format(
        year = now.year,
        month = now.month,
        day = now.day,
        hour = now.hour,
        minute = now.minute,
        second = now.second
    )

    models.init_db(rootDir + '/data.db')

    print("create & init sqlite db ...")
    models.create_tables()
    print("db created")
    print("----------------------------------------")

    # print("inserting parish data ...")
    # insert_parish('../raw/macau-map-data/parishes')
    # os.chdir(rootDir)
    # print("parish datat insertion done.")
    # print("----------------------------------------")

    # print("inserting stats zone data ...")
    # insert_stats_zone('../raw/macau-map-data/stats-zones')
    # os.chdir(rootDir)
    # print("stats zone data insertion done.")
    # print("----------------------------------------")

    # print("inserting bus stop data ...")
    # insert_bus_stops('../raw/macau-bus-data')
    # os.chdir(rootDir)
    # print("bus stop data insertion done.")
    # print("----------------------------------------")

    # print("insert bus route data ...")
    # insert_bus_routes('../raw/macau-bus-data')
    # os.chdir(rootDir)
    # print("bus route data insertion done.")
    # print("----------------------------------------")

    # print("insert bus route point data ...")
    # insert_bus_route_points('../raw/macau-bus-data', True)
    # os.chdir(rootDir)
    # print("bus route data point insertion done.")
    # print("----------------------------------------")

    # print("mapping bus stop to parish ...")
    # parish_bus_stop_mapping()
    # print("mapping bus stop to parish done.")
    # print("----------------------------------------")

    # print("mapping bus stop to stats zone ...")
    # stats_zone_bus_stop_mapping()
    # print("mapping bus stop to stats zone done.")
    # print("----------------------------------------")

    print("calculate distance of route data ...")
    calculate_distance()
    os.chdir(rootDir)
    print("distance of route data insertion done.")
    print("----------------------------------------")

    # print("create bus schedule ...")
    # create_bus_schedule('../raw/macau-bus-data')
    # os.chdir(rootDir)
    # print("bus schedule created.")
    # print("----------------------------------------")


