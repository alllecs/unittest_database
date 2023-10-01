import logging
from psycopg2 import errors, connect
import unittest


# @pytest.mark.parametrize("", [])
class TestPeopleTableInDB(unittest.TestCase):
    connect = None
    table_name = 'People'
    test_data = [1, "Nick", '1990-02-03']

    def setUp(self) -> None:
        logging.info("Connect to database")
        TestPeopleTableInDB.connect = connect(
            host='127.0.0.1',
            port=5432,
            user='postgres',
            password='123',
            dbname='test_db'
        )
        logging.info("Connection successful")

        with TestPeopleTableInDB.connect.cursor() as cursor:
            logging.info("Drop table")
            cursor.execute('DELETE FROM public."People"')
            TestPeopleTableInDB.connect.commit()

        with TestPeopleTableInDB.connect.cursor() as cursor:
            logging.info("Create table")
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS public."People"
            (
                "Index" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1),
                "Name" character varying(255) COLLATE pg_catalog."default" NOT NULL,
                "DataOfBirth" date NOT NULL,
                CONSTRAINT "People_pkey" PRIMARY KEY ("Index")
            )
            """)
            TestPeopleTableInDB.connect.commit()
            logging.info("Success create table!")

    def test_info_table_name(self) -> None:
        """Получить информацию о таблице и проверить название"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute("SELECT * FROM information_schema.columns WHERE table_name = 'People'")
            self.assertIn(TestPeopleTableInDB.table_name, cursor.fetchone())

    def test_table_empty(self) -> None:
        """Проверить, что таблица пустая"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute('SELECT * FROM "People";')
            self.assertEqual(cursor.fetchone(), None)

    def test_correct_insert(self) -> None:
        """Записать корректные данные в таблицу и проверить результат"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute(
                f'INSERT INTO "People"("Name", "DataOfBirth") '
                f'VALUES (\'{TestPeopleTableInDB.test_data[1]}\','f' \'{TestPeopleTableInDB.test_data[2]}\');'
            )
            cursor.execute('SELECT * FROM "People";')
            self.assertEqual((row := cursor.fetchone())[1], TestPeopleTableInDB.test_data[1])
            self.assertEqual(str(row[2]), TestPeopleTableInDB.test_data[2])
            TestPeopleTableInDB.connect.commit()
            cursor.execute('SELECT * FROM "People";')
            self.assertEqual((row := cursor.fetchone())[1], TestPeopleTableInDB.test_data[1])
            self.assertEqual(str(row[2]), TestPeopleTableInDB.test_data[2])

    def test_check_field_name(self) -> None:
        """Получить данные из поля Name"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute('SELECT * FROM "People";')
            self.assertEqual(cursor.fetchone()[1], TestPeopleTableInDB.test_data[1])

    def test_check_field_date_of_birth(self) -> None:
        """Получить данные из поля DateOfBirth"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute('SELECT * FROM "People";')
            self.assertEqual(str(cursor.fetchone()[2]), TestPeopleTableInDB.test_data[2])

    def test_update_field_name(self) -> None:
        """Обновить данные в поле Name"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            TestPeopleTableInDB.test_data[1] = 'Mike'
            cursor.execute(
                'UPDATE "People" SET "Name" = (%s) WHERE "DataOfBirth" = (%s);',
                (TestPeopleTableInDB.test_data[1], TestPeopleTableInDB.test_data[2])
            )
            TestPeopleTableInDB.connect.commit()
            cursor.execute(f'SELECT * FROM "People" WHERE "DataOfBirth" = \'{TestPeopleTableInDB.test_data[2]}\';')
            self.assertEqual(cursor.fetchone()[1], TestPeopleTableInDB.test_data[1])

    def test_update_field_date_of_birth(self) -> None:
        """Обновить данные в поле DateOfBirth"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            TestPeopleTableInDB.test_data[2] = '1980-01-30'
            cursor.execute(
                'UPDATE "People" SET "DataOfBirth" = (%s) WHERE "Name" = (%s);',
                (TestPeopleTableInDB.test_data[2], TestPeopleTableInDB.test_data[1])
            )
            TestPeopleTableInDB.connect.commit()
            cursor.execute(f'SELECT * FROM "People" WHERE "Name" = \'{TestPeopleTableInDB.test_data[1]}\';')
            self.assertEqual(str(cursor.fetchone()[2]), TestPeopleTableInDB.test_data[2])

    @unittest.expectedFailure
    def test_update_null_field_name(self) -> None:
        """Обновить данные в поле Name значением NULL"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute(
                f'UPDATE "People" SET "Name" = NULL WHERE "DataOfBirth" = \'{TestPeopleTableInDB.test_data[2]}\';'
            )

    @unittest.expectedFailure
    def test_update_null_field_date_of_birth(self) -> None:
        """Обновить данные в поле DateOfBirth значением NULL"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute(
                f'UPDATE "People" SET "DataOfBirth" = NULL WHERE "Name" = \'{TestPeopleTableInDB.test_data[1]}\';'
            )

    def test_update_integer_field_date_of_birth(self) -> None:
        """Обновить данные в поле DateOfBirth значением с типом integer"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            try:
                cursor.execute(
                    f'UPDATE "People" SET "DataOfBirth" = 1 WHERE "Name" = \'{TestPeopleTableInDB.test_data[1]}\';',
                )
            except errors.DatatypeMismatch:
                pass

    def test_test_table(self) -> None:
        """Удалить все данные в таблице"""
        with TestPeopleTableInDB.connect.cursor() as cursor:
            cursor.execute('DELETE FROM public."People"')
            TestPeopleTableInDB.connect.commit()
            cursor.execute('SELECT * FROM "People";')
            self.assertEqual(cursor.fetchone(), None)

    def tearDown(self) -> None:
        logging.info("Disconnection to database")
        TestPeopleTableInDB.connect.close()
        logging.info("Disconnection successful")
