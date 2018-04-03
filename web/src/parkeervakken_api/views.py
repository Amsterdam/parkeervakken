from django_filters.rest_framework import FilterSet
from django_filters.rest_framework import filters
from django_filters.rest_framework import DjangoFilterBackend

from parkeervakken_api.models import Parkeervak
from datapunt_api.rest import DatapuntViewSet
from parkeervakken_api.serializers import ParkeervakSerializer

from django.contrib.gis.geos import GEOSGeometry
from rest_framework import viewsets
from rest_framework.response import Response
from django.db import connection

from parkeervakken_api.geo_params import get_request_coord
from parkeervakken_api.serializers import SimpleParkeervakSerializer
from parkeervakken_api.serializers import GeoSelectionSerializer


class ParkeervakFilter(FilterSet):
    id = filters.CharFilter()

    class Meta(object):
        model = Parkeervak
        fields = (
            'id',
            'buurtcode',
            'stadsdeel',
            'straatnaam',
            'soort',
            'aantal',
            'type',
            'e_type',
        )


class ParkeervakList(DatapuntViewSet):
    queryset = Parkeervak.objects.all()
    serializer_detail_class = ParkeervakSerializer
    serializer_class = ParkeervakSerializer
    filter_backends = (DjangoFilterBackend,)
    filter_class = ParkeervakFilter
    queryset_detail = (Parkeervak.objects.all())


class GeoSearchViewSet(viewsets.ViewSet):
    """
    Given a query parameter ``lat/lon` or `x/y combo`
    this will return the parkeervak at that location
    And empty collection is returned when the point is
    not enclosed in any parkeervak.

    http://localhost:8000/parkeervakken/geosearch/?x=129569.42&y=479968.42

    """
    url_name = 'geosearch'

    def list(self, request):
        x, y = get_request_coord(request.query_params)
        if not x or not y:
            return Response([])

        # https://gis.stackexchange.com/questions/206378/st-geomfromtext-not-found

        try:
            selection = Parkeervak.objects.extra(
                where=["ST_Contains(geometrie, ST_GeomFromText('POINT(%s %s)', 28992))"], params=[x, y]).all()[0]  # noqa
            serializer = SimpleParkeervakSerializer(selection)
            return Response([serializer.data])
        except IndexError:
            return Response([])


class GeoSelectionViewSet(viewsets.ViewSet):
    """
    Given a query parameter `straatnam` this will
    return the shape of all the parkeervakken
    at that straatnaam

    /parkeervakken/geoselection/?straatnaam=Zonnehof

    /parkeervakken/geoselection/?ids= 129643479988,129643479977,129641479976,129639479975,129637479974,129626479980,129628479981,129633479983,129635479984,129630479982,129635479972,129627479968,129629479969,129631479970
    """
    url_name = 'geoselection'

    def list(self, request):
        if 'straatnaam' in request.query_params:
            sql = """SELECT Count(*) as aantal, ST_Multi(ST_Union(p.geometrie)) as singleshape
FROM bv.geo_parkeervakken p WHERE straatnaam = %s"""
            params = (request.query_params['straatnaam'],)
        elif 'ids' in request.query_params:
            ids = request.query_params['ids'].split(',')
            sql = """SELECT Count(*) as aantal, ST_Multi(ST_Union(p.geometrie)) as singleshape
FROM bv.geo_parkeervakken p WHERE id in (%s)"""
            format_strings = ','.join(['%s'] * len(ids))
            sql = sql % format_strings
            params = tuple(ids)
        else:
            return Response([])

        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            for row in cursor.fetchall():
                if row[0] == 0:
                    return Response([])
                serializer = GeoSelectionSerializer({'aantal': row[0], 'singleshape': GEOSGeometry(row[1])})
                return Response(serializer.data)
