"""parkeervakken URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.9/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
from rest_framework import response, schemas
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.renderers import CoreJSONRenderer

from rest_framework import routers

from . import views as api_views
from geo_views import views as geo_views


class ParkeervakkenView(routers.APIRootView):
    """
    De parkeervakken in de stad worden hier als een lijst getoond.
    """


class ParkeerVakkenRouter(routers.DefaultRouter):
    APIRootView = ParkeervakkenView


parkeervakken = ParkeerVakkenRouter()

parkeervakken.register(r'parkeervakken', api_views.ParkeervakList,
                       base_name='parkeervak')

# parkeervakken.register(r'geosearch', geo_views.MonumentViewSet,
#                        base_name='parkeervakken')
# parkeervakken.register(r'geoselection', geo_views.ComplexViewSet,
#                        base_name='parkeervakken')
#
# parkeervakken.register(r'typeahead', search_views.TypeaheadViewSet,
#                     base_name='typeahead')
# parkeervakken.register(r'search', search_views.SearchParkeervakkenViewSet,
#                     base_name='search')


urls = parkeervakken.urls


urlpatterns = [
    url(r'^parkeervakken/', include(urls)),
]

@api_view()
@renderer_classes([CoreJSONRenderer])
def monumenten_schema_view(request):
    generator = schemas.SchemaGenerator(
        title='Geo Endpoints',
        patterns=urlpatterns
    )
    return response.Response(generator.get_schema(request=request))

