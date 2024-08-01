#!/usr/bin/env python3
# coding: utf-8

import json
import os
import sys
import time
import testgres
import unittest

from tempfile import mkdtemp
from testgres.utils import get_pg_config

from .base_test import BaseTest, generate_string


@staticmethod
def extension_installed(name: str) -> bool:
	if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
		dlsuffix = 'dll'
	elif sys.platform.startswith("darwin"):
		dlsuffix = 'dylib'
	else:
		dlsuffix = 'so'
	pkg_lib_dir = get_pg_config()["PKGLIBDIR"]
	path = os.path.join(pkg_lib_dir, f'{name}.{dlsuffix}')
	return os.path.isfile(path)


class LogicalTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")

	def squashLogicalChanges(self, rows):
		result = ''
		for row in rows:
			line = row[2]
			if line.startswith('BEGIN') or line.startswith('COMMIT'):
				line = line[0:line.index(' ')]
			result = result + line + "\n"
		return result

	@unittest.skipIf(not extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_simple(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE data(id serial primary key, data text) USING orioledb;\n"
		)

		node.safe_psql(
		    'postgres',
		    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
		)

		node.safe_psql(
		    'postgres', "BEGIN;\n"
		    "INSERT INTO data(data) VALUES('1');\n"
		    "INSERT INTO data(data) VALUES('2');\n"
		    "COMMIT;\n")

		result = self.squashLogicalChanges(
		    node.execute(
		        "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
		    ))
		self.assertEqual(
		    result,
		    "BEGIN\ntable public.data: INSERT: id[integer]:1 data[text]:'1'\ntable public.data: INSERT: id[integer]:2 data[text]:'2'\nCOMMIT\n"
		)

	@unittest.skipIf(not extension_installed("wal2json"),
	                 "'wal2json' is not installed")
	def test_wal2json(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE data(
				id serial primary key,
				data text
			) USING orioledb;
		""")

		node.safe_psql(
		    "SELECT * FROM pg_create_logical_replication_slot('slot', 'wal2json');"
		)

		with node.connect() as con1:
			con1.execute("INSERT INTO data(data) VALUES('1');")
			con1.execute("INSERT INTO data(data) VALUES('2');")
			con1.execute("UPDATE data SET data = 'NO' WHERE id = 1;")
			con1.execute("DELETE FROM data WHERE id = 1;")
			con1.commit()

		result = node.execute(
		    "SELECT * FROM pg_logical_slot_get_changes('slot', NULL, NULL);")
		self.assertDictEqual(
		    json.loads(result[0][2]), {
		        "change": [{
		            "kind": "insert",
		            "schema": "public",
		            "table": "data",
		            "columnnames": ["id", "data"],
		            "columntypes": ["integer", "text"],
		            "columnvalues": [1, "1"]
		        }, {
		            "kind": "insert",
		            "schema": "public",
		            "table": "data",
		            "columnnames": ["id", "data"],
		            "columntypes": ["integer", "text"],
		            "columnvalues": [2, "2"]
		        }, {
		            'kind': 'update',
		            'schema': 'public',
		            'table': 'data',
		            'columnnames': ['id', 'data'],
		            'columntypes': ['integer', 'text'],
		            'columnvalues': [1, 'NO'],
		            'oldkeys': {
		                'keynames': ['id'],
		                'keytypes': ['integer'],
		                'keyvalues': [1]
		            }
		        }, {
		            'kind': 'delete',
		            'schema': 'public',
		            'table': 'data',
		            'oldkeys': {
		                'keynames': ['id'],
		                'keytypes': ['integer'],
		                'keyvalues': [1]
		            }
		        }]
		    })

	def test_logical_subscription(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id serial primary key,
						data text
					) USING orioledb;
					CREATE TABLE o_test2 (
						id serial primary key,
						data text
					) USING orioledb;
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub', tables=['o_test1'])
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute("INSERT INTO o_test1 (data) VALUES('1');")
						con2.execute("INSERT INTO o_test1 (data) VALUES('2');")
						con1.execute("INSERT INTO o_test1 (data) VALUES('3');")
						con2.execute("INSERT INTO o_test1 (data) VALUES('4');")

						con1.execute("INSERT INTO o_test2 (data) VALUES('1');")
						con2.execute("INSERT INTO o_test2 (data) VALUES('2');")
						con1.execute("INSERT INTO o_test2 (data) VALUES('3');")
						con2.execute("INSERT INTO o_test2 (data) VALUES('4');")

						con1.commit()
						con2.commit()

						con1.execute(
						    "UPDATE o_test1 SET data = 'YES' WHERE id = 1;")
						con2.execute(
						    "UPDATE o_test2 SET data = 'YES' WHERE id = 1;")

						con1.execute(
						    "UPDATE o_test1 SET data = 'NO' WHERE id = 4;")
						con2.execute(
						    "UPDATE o_test2 SET data = 'NO' WHERE id = 4;")

						con1.execute("DELETE FROM o_test1 WHERE id = 1;")
						con2.execute("DELETE FROM o_test2 WHERE id = 2;")
						con1.execute("DELETE FROM o_test1 WHERE id = 3;")
						con2.execute("DELETE FROM o_test2 WHERE id = 4;")

						con1.commit()
						con2.commit()

					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
					    [(2, '2'), (4, 'NO')])
					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test2 ORDER BY id'),
					    [(1, 'YES'), (3, '3')])

					# wait until changes apply on subscriber and check them
					sub.catchup()
					# sub.poll_query_until("SELECT orioledb_recovery_synchronized();", expected=True)
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [(2, '2'),
					                                               (4, 'NO')])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test2 ORDER BY id'), [])

	def test_logical_subscription2(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id int primary key,
						data text
					) USING orioledb;
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub')
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					con1.execute("INSERT INTO o_test1 VALUES(1, '1');")
					con1.execute("INSERT INTO o_test1 VALUES(2, '2');")
					con1.execute("INSERT INTO o_test1 VALUES(3, '3');")
					con1.execute("INSERT INTO o_test1 VALUES(4, '4');")
					con1.execute(
					    "UPDATE o_test1 SET data = 'YES' WHERE id = 1;")
					con1.execute(
					    "UPDATE o_test1 SET data = 'NO' WHERE id = 4;")
					con1.execute("DELETE FROM o_test1 WHERE id = 1;")
					con1.execute("DELETE FROM o_test1 WHERE id = 3;")
					con1.commit()

					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
					    [(2, '2'), (4, 'NO')])
					sub.catchup()
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [(2, '2'),
					                                               (4, 'NO')])

					publisher.execute("TRUNCATE o_test1;")
					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
					    [])
					sub.catchup()
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [])

					con1.execute("INSERT INTO o_test1 VALUES(1, '1');")
					con1.execute("INSERT INTO o_test1 VALUES(2, '2');")
					con1.execute("INSERT INTO o_test1 VALUES(3, '3');")
					con1.execute("INSERT INTO o_test1 VALUES(4, '4');")
					con1.execute(
					    "UPDATE o_test1 SET data = 'YES' WHERE id = 1;")
					con1.execute(
					    "UPDATE o_test1 SET data = 'NO' WHERE id = 4;")
					con1.execute("DELETE FROM o_test1 WHERE id = 2;")
					con1.execute("DELETE FROM o_test1 WHERE id = 4;")
					con1.commit()

				self.assertListEqual(
				    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
				    [(1, 'YES'), (3, '3')])
				sub.catchup()
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM o_test1 ORDER BY id'),
				    [(1, 'YES'), (3, '3')])

	def test_logical_create_new_table(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id int primary key,
						data int
					) USING orioledb;
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub')
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					con1.execute("INSERT INTO o_test1 VALUES(1, '1');")
					con1.execute("INSERT INTO o_test1 VALUES(2, '2');")
					con1.execute("INSERT INTO o_test1 VALUES(3, '3');")
					con1.execute("INSERT INTO o_test1 VALUES(4, '4');")
					con1.execute(
					    "UPDATE o_test1 SET data = '100' WHERE id = 1;")
					con1.execute(
					    "UPDATE o_test1 SET data = '500' WHERE id = 4;")
					con1.execute("DELETE FROM o_test1 WHERE id = 1;")
					con1.execute("DELETE FROM o_test1 WHERE id = 3;")

					con1.execute("INSERT INTO o_test1 VALUES(11, '11');")
					con1.execute("INSERT INTO o_test1 VALUES(12, '12');")
					con1.execute("INSERT INTO o_test1 VALUES(13, '13');")
					con1.execute("INSERT INTO o_test1 VALUES(14, '14');")

					con1.execute(
					    "UPDATE o_test1 SET data = '500' WHERE id = 14;")
					con1.execute(
					    "UPDATE o_test1 SET data = '100' WHERE id = 11;")
					con1.execute("ALTER TABLE o_test1 ADD COLUMN val2 int;")
					con1.execute("UPDATE o_test1 SET val2 = id + 1000;")
					con1.execute("""
						CREATE TABLE o_test2 (
							id int primary key,
							data int
						) USING orioledb;
					""")
					con1.commit()

					subscriber.execute(
					    "ALTER TABLE o_test1 ADD COLUMN val2 char(4);")

					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
					    [(2, 2, 1002), (4, 500, 1004), (11, 100, 1011),
					     (12, 12, 1012), (13, 13, 1013), (14, 500, 1014)])
					sub.catchup()
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'),
					    [(2, 2, '1002'), (4, 500, '1004'), (11, 100, '1011'),
					     (12, 12, '1012'), (13, 13, '1013'),
					     (14, 500, '1014')])

	def test_logical_drop_table(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id int primary key,
						data int
					) USING orioledb;
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub')
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					con1.execute("INSERT INTO o_test1 VALUES(1, '1');")
					con1.execute("INSERT INTO o_test1 VALUES(2, '2');")
					con1.execute("INSERT INTO o_test1 VALUES(3, '3');")
					con1.execute("INSERT INTO o_test1 VALUES(4, '4');")
					con1.execute(
					    "UPDATE o_test1 SET data = '100' WHERE id = 1;")
					con1.execute(
					    "UPDATE o_test1 SET data = '500' WHERE id = 4;")
					con1.execute("DELETE FROM o_test1 WHERE id = 1;")
					con1.execute("DELETE FROM o_test1 WHERE id = 3;")

					con1.execute("INSERT INTO o_test1 VALUES(11, '11');")
					con1.execute("INSERT INTO o_test1 VALUES(12, '12');")
					con1.execute("INSERT INTO o_test1 VALUES(13, '13');")
					con1.execute("INSERT INTO o_test1 VALUES(14, '14');")

					con1.execute(
					    "UPDATE o_test1 SET data = '500' WHERE id = 14;")
					con1.execute(
					    "UPDATE o_test1 SET data = '100' WHERE id = 11;")
					con1.execute("ALTER TABLE o_test1 ADD COLUMN val2 int;")
					con1.execute("UPDATE o_test1 SET val2 = id + 1000;")
					con1.execute("""
						DROP TABLE o_test1;
					""")
					con1.commit()

					publisher_dead = False
					repeats = 5
					for _ in range(0, repeats):
						try:
							if con1.execute("""
								SELECT * FROM pg_stat_activity WHERE application_name = 'test_sub';
							""") == []:
								publisher_dead = True
						except:
							publisher_dead = True
						time.sleep(0.1)

					self.assertFalse(publisher_dead)

					subscriber.execute(
					    "ALTER TABLE o_test1 ADD COLUMN val2 char(4);")

					sub.catchup()
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'),
					    [(2, 2, '1002'), (4, 500, '1004'), (11, 100, '1011'),
					     (12, 12, '1012'), (13, 13, '1013'),
					     (14, 500, '1014')])

	def test_logical_toast(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id integer PRIMARY KEY,
						v1 text,
						v2 text,
						v3 text
					) USING orioledb;
				"""

				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub')
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					for i in range(0, 10):
						baseIndex = i * 3 + 1
						con1.execute(
						    "INSERT INTO o_test1 VALUES (%d, '%s', '%s', '%s')"
						    % (i + 1, generate_string(2500, baseIndex),
						       generate_string(2500, baseIndex + 1),
						       generate_string(2500, baseIndex + 2)))

					con1.commit()

					publisher_dead = False
					repeats = 5
					for _ in range(0, repeats):
						try:
							if con1.execute("""
								SELECT * FROM pg_stat_activity WHERE application_name = 'test_sub';
							""") == []:
								publisher_dead = True
						except:
							publisher_dead = True
						time.sleep(0.1)

					self.assertFalse(publisher_dead)

					sub.catchup()
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'),
					    [(2, 2, '1002'), (4, 500, '1004'), (11, 100, '1011'),
					     (12, 12, '1012'), (13, 13, '1013'),
					     (14, 500, '1014')])

	def test_logical_index(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id integer PRIMARY KEY,
						val integer
					) USING orioledb;
					CREATE INDEX o_test1_idx ON o_test1 (val);
				"""

				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub')
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					con1.execute("INSERT INTO o_test1 VALUES(1, 20);")
					con1.execute("INSERT INTO o_test1 VALUES(2, 17);")
					con1.execute("INSERT INTO o_test1 VALUES(3, 19);")
					con1.execute("INSERT INTO o_test1 VALUES(4, 18);")
					con1.commit()

					plan = publisher.execute("""
						EXPLAIN (COSTS OFF, FORMAT JSON)
							SELECT * FROM o_test1 ORDER BY val;
					""")[0][0][0]["Plan"]
					self.assertEqual('Custom Scan', plan["Node Type"])
					self.assertEqual('o_test1', plan['Relation Name'])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test1 ORDER BY val'), [(2, 17),
					                                                (4, 18),
					                                                (3, 19),
					                                                (1, 20)])
					sub.catchup()
					plan = subscriber.execute("""
						EXPLAIN (COSTS OFF, FORMAT JSON)
							SELECT * FROM o_test1 ORDER BY val;
					""")[0][0][0]["Plan"]
					self.assertEqual('Custom Scan', plan["Node Type"])
					self.assertEqual('o_test1', plan['Relation Name'])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY val'), [(2, 17),
					                                                (4, 18),
					                                                (3, 19),
					                                                (1, 20)])

	def test_logical_rollback(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id integer PRIMARY KEY,
						val integer
					) USING orioledb;
				"""

				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub')
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					con1.execute("INSERT INTO o_test1 VALUES(1, 20);")
					con1.execute("INSERT INTO o_test1 VALUES(2, 17);")
					con1.execute("INSERT INTO o_test1 VALUES(3, 19);")
					con1.execute("INSERT INTO o_test1 VALUES(4, 18);")
					con1.rollback()

					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
					    [])
					sub.catchup()
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [])

	def test_logical_savepoint(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id integer PRIMARY KEY,
						val integer
					) USING orioledb;
				"""

				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub')
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					con1.execute("INSERT INTO o_test1 VALUES(1, 20);")
					con1.execute("INSERT INTO o_test1 VALUES(2, 17);")
					con1.execute("SAVEPOINT s1;")
					con1.execute("INSERT INTO o_test1 VALUES(3, 19);")
					con1.execute("INSERT INTO o_test1 VALUES(4, 18);")
					con1.execute("ROLLBACK TO SAVEPOINT s1;")
					con1.commit()

					publisher_dead = False
					repeats = 5
					for _ in range(0, repeats):
						try:
							if con1.execute("""
								SELECT * FROM pg_stat_activity WHERE application_name = 'test_sub';
							""") == []:
								publisher_dead = True
						except:
							publisher_dead = True
						time.sleep(0.1)

					self.assertFalse(publisher_dead)

					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
					    [(1, 20), (2, 17)])
					sub.catchup()
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [(1, 20), (2, 17)])
