from collections import OrderedDict

from rest_framework import renderers, pagination, response, \
    viewsets, filters
from rest_framework import serializers
from rest_framework.utils.urls import replace_query_param

from rest_framework_extensions.mixins import DetailSerializerMixin

DEFAULT_RENDERERS = (renderers.JSONRenderer, renderers.BrowsableAPIRenderer)
FORMATS = [dict(format=r.format, type=r.media_type) for r in DEFAULT_RENDERERS]


#
# class DataSetSerializerMixin(object):
#     def to_representation(self, obj):
#         result = super().to_representation(obj)
#         result['dataset'] = self.dataset
#         return result
#


class LinksField(serializers.HyperlinkedIdentityField):

    def to_representation(self, value):
        request = self.context.get('request')

        result = OrderedDict([
            ('self', dict(
                href=self.get_url(value, self.view_name, request, None))),
        ])

        return result


class HALSerializer(serializers.HyperlinkedModelSerializer):
    url_field_name = '_links'
    serializer_url_field = LinksField


class HALPagination(pagination.PageNumberPagination):
    page_size_query_param = 'page_size'

    def get_paginated_response(self, data):
        self_link = self.request.build_absolute_uri()
        if self_link.endswith(".api"):
            self_link = self_link[:-4]

        if self.page.has_next():
            next_link = replace_query_param(
                self_link, self.page_query_param, self.page.next_page_number())
        else:
            next_link = None

        if self.page.has_previous():
            prev_link = replace_query_param(
                self_link, self.page_query_param,
                self.page.previous_page_number())
        else:
            prev_link = None

        return response.Response(OrderedDict([
            ('_links', OrderedDict([
                ('self', dict(href=self_link)),
                ('next', dict(href=next_link)),
                ('previous', dict(href=prev_link)),
            ])),
            ('count', self.page.paginator.count),
            ('results', data)
        ]))


class DatapuntViewSet(DetailSerializerMixin, viewsets.ReadOnlyModelViewSet):
    renderer_classes = DEFAULT_RENDERERS
    pagination_class = HALPagination
    filter_backends = (filters.SearchFilter)


class DisplayField(serializers.Field):

    def __init__(self, *args, **kwargs):
        kwargs['source'] = '*'
        kwargs['read_only'] = True
        super().__init__(*args, **kwargs)

    def to_representation(self, value):
        return str(value)
