import os
import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration

from parkeervakken_api.settings_common import *  # noqa F403
from parkeervakken_api.settings_common import INSTALLED_APPS

from parkeervakken_api.settings_databases import LocationKey, \
    get_docker_host, \
    get_database_key, \
    OVERRIDE_HOST_ENV_VAR, \
    OVERRIDE_PORT_ENV_VAR

# Application definition

INSTALLED_APPS += [
    'parkeervakken_api',
]

ROOT_URLCONF = 'parkeervakken_api.urls'

WSGI_APPLICATION = 'parkeervakken_api.wsgi.application'


DATABASE_OPTIONS = {
    LocationKey.docker: {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': os.getenv('DATABASE_NAME', 'parkeervakken'),
        'USER': os.getenv('DATABASE_USER', 'parkeervakken'),
        'PASSWORD': os.getenv('DATABASE_PASSWORD', 'insecure'),
        'HOST': 'database',
        'PORT': '5432'
    },
    LocationKey.local: {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': os.getenv('DATABASE_NAME', 'parkeervakken'),
        'USER': os.getenv('DATABASE_USER', 'parkeervakken'),
        'PASSWORD': os.getenv('DATABASE_PASSWORD', 'insecure'),
        'HOST': get_docker_host(),
        'PORT': '5409'
    },
    LocationKey.override: {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': os.getenv('DATABASE_NAME', 'parkeervakken'),
        'USER': os.getenv('DATABASE_USER', 'parkeervakken'),
        'PASSWORD': os.getenv('DATABASE_PASSWORD', 'insecure'),
        'HOST': os.getenv(OVERRIDE_HOST_ENV_VAR),
        'PORT': os.getenv(OVERRIDE_PORT_ENV_VAR, '5432')
    },
}

# Database

DATABASES = {
    'default': DATABASE_OPTIONS[get_database_key()]
}

HEALTH_MODEL = 'parkeervakken_api.Parkeervak'

SENTRY_DSN = os.getenv('SENTRY_DSN')
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment="parkeervakken",
        integrations=[DjangoIntegration()]
    )
