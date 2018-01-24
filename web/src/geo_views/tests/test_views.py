from django.db import connection
from django.test import TestCase
from django.core.management import call_command

from datasets.schiphol.tests import factories as schiphol_factories


class ViewsTest(TestCase):
    def setUp(self):
        self.schiphol_point = schiphol_factories.HoogtebeperkendeVlakkenPointFactory.create(pk=1)
        self.schiphol_line = schiphol_factories.HoogtebeperkendeVlakkenLineFactory.create(pk=2)
        self.schiphol_polygon = schiphol_factories.HoogtebeperkendeVlakkenPolyFactory.create(pk=3)

        call_command('sync_views')

    def get_row(self, view_name):
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM " + str(view_name) + " LIMIT 1")
        result = cursor.fetchone()
        self.assertIsNotNone(result)

        return dict(zip([col[0] for col in cursor.description], result))

    # schiphol
    def test_point_view(self):
        row = self.get_row('geo_schiphol_hoogtebeperkendevlakken_point')
        self.assertEqual(row['id'], self.schiphol_point.id)
        self.assertNotEqual(row['geometrie'], None)

    def test_line_view(self):
        row = self.get_row('geo_schiphol_hoogtebeperkendevlakken_line')
        self.assertEqual(row['id'], self.schiphol_line.id)
        self.assertNotEqual(row['geometrie'], None)

    def test_polygon_view(self):
        row = self.get_row('geo_schiphol_hoogtebeperkendevlakken_polygon')
        self.assertEqual(row['id'], self.schiphol_polygon.id)
        self.assertNotEqual(row['geometrie'], None)
