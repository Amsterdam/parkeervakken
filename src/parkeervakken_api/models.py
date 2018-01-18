from django.contrib.gis.db import models


class Parkeervak(models.Model):
    """
    Parkeervak model
    """
    class Meta:
        db_table = 'parkeervakken'

    id = models.CharField(max_length=30, primary_key=True, name='parkeer_id')
    buurtcode = models.CharField(max_length=20, null=True)
    # stadsdeel = models.CharField(max_length=20, null=True)
    straatnaam = models.CharField(max_length=40, null=True)
    aantal = models.IntegerField(null=True)
    type = models.CharField(max_length=20, null=True)
    e_type = models.CharField(max_length=5, null=True)
    bord = models.CharField(max_length=50, null=True)
    geometrie = models.MultiPolygonField(name='geom')

    # def __str__(self):
    #
    #     return f"Parkeervak {self.id} te {self.straatnaam}"
