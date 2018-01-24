from django.contrib.gis.geos import GEOSGeometry
from rest_framework import viewsets
from rest_framework.response import Response
from django.db import connection

from geo_views.geo_params import get_request_coord
from parkeervakken_api.models import Parkeervak
from parkeervakken_api.serializers import ParkeervakSerializer, GeoSelectionSerializer


class GeoSearchViewSet(viewsets.ViewSet):
    """
    Given a query parameter ``lat/lon` or `x/y combo` this will return the parkeervak at that location
    And empty collection is returned when the point is not enclosed in any parkeervak.

    http://localhost:8000/parkeervakken/geosearch/?x=129569.42&y=479968.42

    """
    url_name = 'geosearch'

    def list(self, request):
        x, y = get_request_coord(request.query_params)
        if not x or not y:
            return Response([])

        try:
            selection = Parkeervak.objects.extra(where=["ST_Contains(geom, ST_GeomFromText('POINT(%s %s)', 28992))"], params=[x, y]).all()[0]
            serializer = ParkeervakSerializer(selection)
            return Response([serializer.data])
        except IndexError:
            return Response([])

class GeoSelectionViewSet(viewsets.ViewSet):
    """
    Given a query parameter `straatnam` this will return the shape of all the parkeervakken at that straatnaam

    http://localhost:8000/parkeervakken/geoselection/?straatnaam=Zonnehof

    http://localhost:8000/parkeervakken/geoselection/?ids= 129643479988,129643479977,129641479976,129639479975,129637479974,129626479980,129628479981,129633479983,129635479984,129630479982,129635479972,129627479968,129629479969,129631479970
    """
    url_name = 'geoselection'

    def list(self, request):
        if 'straatnaam' in request.query_params:
            straatnaam = request.query_params['straatnaam']
            sql = """SELECT Count(*) as aantal, ST_Multi(ST_Union(p.geom)) as singleshape
FROM parkeervakken p WHERE straatnaam = %s"""
            with connection.cursor() as cursor:
                cursor.execute(sql, (straatnaam,))
                for row in cursor.fetchall():
                    serializer = GeoSelectionSerializer({'aantal': row[0], 'singleshape': GEOSGeometry(row[1])})
                    return Response(serializer.data)

        if 'ids' in request.query_params:
            ids = request.query_params['ids'].split(',')
            sql = """SELECT Count(*) as aantal, ST_Multi(ST_Union(p.geom)) as singleshape
FROM parkeervakken p WHERE parkeer_id in (%s)"""
            format_strings = ','.join(['%s'] * len(ids))
            with connection.cursor() as cursor:
                cursor.execute(sql % format_strings, tuple(ids))
                for row in cursor.fetchall():
                    serializer = GeoSelectionSerializer({'aantal': row[0], 'singleshape': GEOSGeometry(row[1])})
                    return Response(serializer.data)

        return Response([])
