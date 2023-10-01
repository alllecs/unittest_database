import unittest
import psycopg2
import logging


class TestPeopleTableInDB(unittest.TestCase):
    connect = None

    def setUp(self) -> None:
        print("connect")
        logging.info("Connect to database")
        TestPeopleTableInDB.connect = psycopg2.connect(
            host='127.0.0.1',
            port=5432,
            user='postgres',
            password='123',
            dbname='test_db'
        )
        logging.info("Connection successful")

    def test_check_select(self):
        cursor = TestPeopleTableInDB.connect.cursor()
        cursor.execute('SELECT * FROM "People" WHERE "Index" = 1;')
        row = cursor.fetchone()
        print(row)
        cursor.close()

        with self.subTest("Message for this subtest"):
            self.assertEqual(4 % 2, 0)

        print(123)

    def tearDown(self) -> None:
        TestPeopleTableInDB.connect.close()
        print("close")
