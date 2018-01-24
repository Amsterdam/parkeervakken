from unittest import TestCase
import datetime

import psycopg2

import data_import


class TestProcessing(TestCase):
    def setUp(self):
        database = 'test_parkeervakken2'
        user = 'test'
        password = 'test'
        host = 'localhost'
        port = 5432

        conn = psycopg2.connect(database=database,
                                user=user,
                                password=password,
                                host=host,
                                port=port)

        cur = conn.cursor()
        self.conn = conn
        self.cur = cur

        force_drop = True
        schema = 'public'
        hist_table = 'h_parkeervakken'
        park_table = 'parkeervakken'
        res_table = 'reserveringen'
        date_table = 'datums'
        data_import.create_hist_table(conn,
                                      cur,
                                      hist_table,
                                      schema,
                                      force_drop)
        data_import.create_parking_space_table(conn,
                                               cur,
                                               park_table,
                                               schema,
                                               force_drop)
        data_import.create_parking_reservation_table(conn,
                                                     cur,
                                                     res_table,
                                                     schema,
                                                     force_drop)
        data_import.create_date_table(conn,
                                      cur,
                                      date_table,
                                      schema,
                                      force_drop)

    def insert_history_data(self, data):
        """
        :type data: list
        """

        insert_hist = """INSERT INTO h_parkeervakken
        (
            parkeer_id,
            buurtcode,
            straatnaam,
            soort,
            type,
            aantal,
            kenteken,
            e_type,
            bord,
            begintijd1,
            eindtijd1,
            ma_vr,
            ma_za,
            zo,
            ma,
            di,
            wo,
            "do",
            vr,
            za,
            eindtijd2,
            begintijd2,
            opmerking,
            tvm_begind,
            tvm_eindd,
            tvm_begint,
            tvm_eindt,
            tvm_opmerk,
            geom,
            stadsdeel,
            goedkeurings_datum
        )
        VALUES
        (
            %(park_id)s,
            %(neighbourhood)s,
            %(street)s,
            %(kind)s,
            %(type)s,
            %(number)s,
            %(sign)s,
            %(e_type)s,
            %(plate)s,
            %(starttime1)s,
            %(endtime1)s,
            %(ma_vr)s,
            %(ma_za)s,
            %(zo)s,
            %(ma)s,
            %(di)s,
            %(wo)s,
            %(do)s,
            %(vr)s,
            %(za)s,
            %(endtime2)s,
            %(begintime2)s,
            %(remark)s,
            %(tvm_begin_date)s,
            %(tvm_end_date)s,
            %(tvm_begin_time)s,
            %(tvm_end_time)s,
            %(tvm_remark)s,
            %(geom)s,
            %(city_part)s,
            %(approval_date)s
        )"""

        try:
            self.cur.executemany(insert_hist, data)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            self.conn.close()
            raise

    def test_reservation_garbage(self):
        # FOR FISCAAL and MULDER
        pass

    def test_reservation_parking_id(self):
        pass

    def test_fiscal_hours(self):
        hist_table = 'h_parkeervakken'
        res_table = 'reserveringen'
        date_table = 'datums'
        schema = 'public'

        hist_data = [
            {
                'park_id': 'p1',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'FISCAAL',
                'type': 'type1',
                'number': 1,
                'sign': None,
                'e_type': None,
                'plate': None,
                'starttime1': None,
                'endtime1': None,
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': datetime.datetime(2010, 3, 1),
                'tvm_end_date': datetime.datetime(2010, 3, 1),
                'tvm_begin_time': '09:00',
                'tvm_end_time': '12:00',
                'tvm_remark': 'o1',
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
            {
                'park_id': 'p2',
                'neighbourhood': 'b2',
                'street': 's2',
                'kind': 'FISCAAL',
                'type': 'type2',
                'number': 1,
                'sign': None,
                'e_type': None,
                'plate': None,
                'starttime1': None,
                'endtime1': None,
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': datetime.datetime(2010, 3, 1),
                'tvm_end_date': datetime.datetime(2010, 3, 2),
                'tvm_begin_time': '09:00',
                'tvm_end_time': '12:00',
                'tvm_remark': 'o2',
                'geom': None,
                'city_part': 'cp2',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
            {
                'park_id': 'p3',
                'neighbourhood': 'b3',
                'street': 's3',
                'kind': 'FISCAAL',
                'type': 'type3',
                'number': 1,
                'sign': None,
                'e_type': None,
                'plate': None,
                'starttime1': None,
                'endtime1': None,
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': datetime.datetime(2010, 3, 1),
                'tvm_end_date': datetime.datetime(2010, 3, 2),
                'tvm_begin_time': '09:00',
                'tvm_end_time': '08:00',
                'tvm_remark': 'o3',
                'geom': None,
                'city_part': 'cp3',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
            {
                'park_id': 'p4',
                'neighbourhood': 'b4',
                'street': 's4',
                'kind': 'FISCAAL',
                'type': 'type4',
                'number': 1,
                'sign': None,
                'e_type': None,
                'plate': None,
                'starttime1': None,
                'endtime1': None,
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': datetime.datetime(2010, 3, 1),
                'tvm_end_date': datetime.datetime(2010, 3, 4),
                'tvm_begin_time': '09:00',
                'tvm_end_time': '09:00',
                'tvm_remark': 'o4',
                'geom': None,
                'city_part': 'cp4',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
            {
                'park_id': 'p1',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'FISCAAL',
                'type': 'type4',
                'number': 1,
                'sign': None,
                'e_type': None,
                'plate': None,
                'starttime1': None,
                'endtime1': None,
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': datetime.datetime(2010, 2, 1),
                'tvm_end_date': datetime.datetime(2010, 2, 1),
                'tvm_begin_time': '09:00',
                'tvm_end_time': '12:00',
                'tvm_remark': 'o11',
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2009, 12, 28),
            },
            {
                'park_id': 'p5',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'FISCAAL',
                'type': 'type4',
                'number': 1,
                'sign': None,
                'e_type': None,
                'plate': None,
                'starttime1': None,
                'endtime1': None,
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': datetime.datetime(2010, 2, 1),
                'tvm_end_date': datetime.datetime(2010, 1, 1),
                'tvm_begin_time': '09:00',
                'tvm_end_time': '12:00',
                'tvm_remark': 'o11',
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2009, 12, 28),
            },
            {
                'park_id': 'p6',
                'neighbourhood': 'b4',
                'street': 's4',
                'kind': 'MULDER',
                'type': 'type4',
                'number': 1,
                'sign': None,
                'e_type': None,
                'plate': None,
                'starttime1': None,
                'endtime1': None,
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': datetime.datetime(2010, 3, 1),
                'tvm_end_date': datetime.datetime(2010, 3, 4),
                'tvm_begin_time': '09:00',
                'tvm_end_time': '09:00',
                'tvm_remark': 'o4',
                'geom': None,
                'city_part': 'cp4',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
        ]

        self.insert_history_data(hist_data)

        data_import.update_dates(self.conn,
                                 self.cur,
                                 date_table,
                                 hist_table,
                                 schema,
                                 interval='1 year')

        data_import.update_fiscal_reservations(self.conn,
                                               self.cur,
                                               hist_table,
                                               res_table,
                                               date_table,
                                               schema)

        query = """SELECT
            parkeer_id,
            reserverings_datum,
            begintijd,
            eindtijd
        FROM reserveringen"""

        try:
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception:
            self.conn.rollback()
            self.conn.close()
            raise

        expected_rows = [
            (
                'p1',
                datetime.datetime(2010, 3, 1),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 2, 1),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 3, 1),
                datetime.time(9, 0),
                datetime.time(0, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 3, 2),
                datetime.time(0, 0),
                datetime.time(12, 0),
            ),
            (
                'p3',
                datetime.datetime(2010, 3, 1),
                datetime.time(9, 0),
                datetime.time(0, 0),
            ),
            (
                'p3',
                datetime.datetime(2010, 3, 2),
                datetime.time(0, 0),
                datetime.time(8, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 3, 1),
                datetime.time(9, 0),
                datetime.time(0, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 3, 2),
                datetime.time(0, 0),
                datetime.time(0, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 3, 3),
                datetime.time(0, 0),
                datetime.time(0, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 3, 4),
                datetime.time(0, 0),
                datetime.time(9, 0),
            ),
        ]

        self.assertEqual(len(expected_rows), len(results))

        for row in expected_rows:
            self.assertIn(row, results)

    def test_fiscal_garbage(self):
        pass

    def test_mulder_days_week(self):
        pass

    def test_mulder_days(self):
        hist_table = 'h_parkeervakken'
        res_table = 'reserveringen'
        date_table = 'datums'
        schema = 'public'

        hist_data = [
            {
                'park_id': 'p1',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'MULDER',
                'type': 'type1',
                'number': 1,
                'sign': None,
                'e_type': 'e1',
                'plate': 'k1',
                'starttime1': '9:00',
                'endtime1': '12:00',
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': None,
                'tvm_end_date': None,
                'tvm_begin_time': None,
                'tvm_end_time': None,
                'tvm_remark': None,
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
            {
                'park_id': 'p1',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'MULDER',
                'type': 'type1',
                'number': 1,
                'sign': None,
                'e_type': 'e1',
                'plate': 'k1',
                'starttime1': '9:00',
                'endtime1': '12:00',
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': None,
                'tvm_end_date': None,
                'tvm_begin_time': None,
                'tvm_end_time': None,
                'tvm_remark': None,
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2009, 12, 31),
            },
            {
                'park_id': 'p2',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'MULDER',
                'type': 'type1',
                'number': 1,
                'sign': None,
                'e_type': 'e1',
                'plate': 'k1',
                'starttime1': '9:00',
                'endtime1': '12:00',
                'ma_vr': True,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': None,
                'tvm_end_date': None,
                'tvm_begin_time': None,
                'tvm_end_time': None,
                'tvm_remark': None,
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
            {
                'park_id': 'p2',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'MULDER',
                'type': 'type1',
                'number': 1,
                'sign': None,
                'e_type': 'e1',
                'plate': 'k1',
                'starttime1': '9:00',
                'endtime1': '12:00',
                'ma_vr': True,
                'ma_za': False,
                'zo': False,
                'ma': False,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': None,
                'tvm_end_date': None,
                'tvm_begin_time': None,
                'tvm_end_time': None,
                'tvm_remark': None,
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2009, 12, 31),
            },
            {
                'park_id': 'p3',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'MULDER',
                'type': 'type1',
                'number': 1,
                'sign': None,
                'e_type': 'e1',
                'plate': 'k1',
                'starttime1': '9:00',
                'endtime1': '12:00',
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': True,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': None,
                'begintime2': None,
                'remark': None,
                'tvm_begin_date': None,
                'tvm_end_date': None,
                'tvm_begin_time': None,
                'tvm_end_time': None,
                'tvm_remark': None,
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
            {
                'park_id': 'p4',
                'neighbourhood': 'b1',
                'street': 's1',
                'kind': 'MULDER',
                'type': 'type1',
                'number': 1,
                'sign': None,
                'e_type': 'e1',
                'plate': 'k1',
                'starttime1': '9:00',
                'endtime1': '12:00',
                'ma_vr': False,
                'ma_za': False,
                'zo': False,
                'ma': True,
                'di': False,
                'wo': False,
                'do': False,
                'vr': False,
                'za': False,
                'endtime2': '15:00',
                'begintime2': '13:00',
                'remark': None,
                'tvm_begin_date': None,
                'tvm_end_date': None,
                'tvm_begin_time': None,
                'tvm_end_time': None,
                'tvm_remark': None,
                'geom': None,
                'city_part': 'cp1',
                'approval_date': datetime.datetime(2010, 1, 1),
            },
        ]

        self.insert_history_data(hist_data)

        data_import.update_dates(self.conn,
                                 self.cur,
                                 date_table,
                                 hist_table,
                                 schema,
                                 interval='19 day')

        data_import.update_mulder_reservations(self.conn,
                                               self.cur,
                                               hist_table,
                                               res_table,
                                               date_table,
                                               schema)

        query = """SELECT
            parkeer_id,
            reserverings_datum,
            begintijd,
            eindtijd
        FROM reserveringen"""

        try:
            self.cur.execute(query)
            results = self.cur.fetchall()
            print(results)
        except Exception:
            self.conn.rollback()
            self.conn.close()
            raise

        expected_rows = [
            (
                'p1',
                datetime.datetime(2010, 1, 1),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 2),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 3),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 4),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 5),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 6),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 7),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 8),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 9),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 10),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 11),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 12),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 13),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 14),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 15),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 16),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 17),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 18),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 19),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p1',
                datetime.datetime(2010, 1, 20),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 1),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 4),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 5),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 6),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 7),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 8),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 11),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 12),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 13),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 14),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 15),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 18),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 19),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p2',
                datetime.datetime(2010, 1, 20),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p3',
                datetime.datetime(2010, 1, 4),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p3',
                datetime.datetime(2010, 1, 11),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p3',
                datetime.datetime(2010, 1, 18),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 1, 4),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 1, 11),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 1, 18),
                datetime.time(9, 0),
                datetime.time(12, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 1, 4),
                datetime.time(13, 0),
                datetime.time(15, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 1, 11),
                datetime.time(13, 0),
                datetime.time(15, 0),
            ),
            (
                'p4',
                datetime.datetime(2010, 1, 18),
                datetime.time(13, 0),
                datetime.time(15, 0),
            ),
        ]

        self.assertEqual(len(expected_rows), len(results))

        for row in expected_rows:
            self.assertIn(row, results)

    def test_parking_data(self):
        pass

    def tearDown(self):
        self.conn.close()
