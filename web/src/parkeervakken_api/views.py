from django_filters.rest_framework import FilterSet
from django_filters.rest_framework import filters
from django_filters.rest_framework import DjangoFilterBackend

from parkeervakken_api.models import Parkeervak
from datapunt_api.rest import DatapuntViewSet
from parkeervakken_api.serializers import ParkeervakSerializer


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
