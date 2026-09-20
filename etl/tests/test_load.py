"""Regression tests for the database-targeted ETL loader."""

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from extract import extract_all
from load import _mysql_url_options, load_all
from transform import transform_all


class TestLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tables = transform_all(extract_all())

    def test_sqlite_loads_star_schema(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "warehouse.db"
            load_all(self.tables, sqlite_path=db_path)
            with sqlite3.connect(db_path) as conn:
                for table_name, dataframe in self.tables.items():
                    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    self.assertEqual(count, len(dataframe))

    def test_mysql_url_options(self):
        options = _mysql_url_options("mysql://etl:p%40ss@db.example:3307/ecommerce?ssl_disabled=true")
        self.assertEqual(options["host"], "db.example")
        self.assertEqual(options["port"], 3307)
        self.assertEqual(options["password"], "p@ss")
        self.assertTrue(options["ssl_disabled"])

    def test_unknown_target_is_rejected(self):
        with self.assertRaises(ValueError):
            load_all(self.tables, target="oracle")


if __name__ == "__main__":
    unittest.main()
