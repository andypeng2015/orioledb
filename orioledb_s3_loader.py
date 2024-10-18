#!/usr/bin/env python3

import argparse
import boto3
import os
import re
import struct
import testgres

from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError
from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig
from threading import Event
from typing import Callable
from urllib.parse import urlparse


class OrioledbS3ObjectLoader:

	def parse_args(self):
		epilog = """
			This util uses boto3 under the hood.
			You can set credentials using AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION variables
			Or by using ~/.aws/config file.
			Read for more details:
			https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables
		"""
		parser = argparse.ArgumentParser(usage=argparse.SUPPRESS,
		                                 epilog=epilog)
		parser.add_argument(
		    '--endpoint',
		    dest='endpoint',
		    required=True,
		    help="AWS url (must contain bucket name if no prefix set)")
		parser.add_argument('-d',
		                    '--data-dir',
		                    dest='data_dir',
		                    required=True,
		                    help="Destination data directory")
		parser.add_argument(
		    '--prefix',
		    dest='prefix',
		    required=False,
		    default="",
		    help="Prefix to prepend to S3 object name (may contain bucket name)"
		)
		parser.add_argument('--cert-file',
		                    dest='cert_file',
		                    help="Path to crt file")
		parser.add_argument(
		    '--verbose',
		    dest='verbose',
		    action='store_true',
		    help="More verbose output. Downloaded files displayed.")

		try:
			args = parser.parse_args()
		except SystemExit as e:
			if e.code != 0:
				parser.print_help()
			raise

		if 'cert_file' in args:
			verify = args.cert_file
		else:
			verify = None

		parsed_url = urlparse(args.endpoint)
		bucket = parsed_url.netloc.split('.')[0]
		raw_endpoint = f"{parsed_url.scheme}://{'.'.join(parsed_url.netloc.split('.')[1:])}"

		splitted_prefix = args.prefix.strip('/').split('/')
		splitted_path = parsed_url.path.strip('/').split('/')
		prefix = os.path.join(*splitted_path, *splitted_prefix)
		splitted_prefix = prefix.split('/')

		bucket_in_endpoint = True
		bucket_in_prefix = False
		try:
			config = Config(s3={'addressing_style': 'virtual'})
			s3_client = boto3.client("s3",
			                         endpoint_url=raw_endpoint,
			                         verify=verify,
			                         config=config)
			s3_client.head_bucket(Bucket=bucket)
			bucket_name = bucket
		except ValueError:
			bucket_in_endpoint = False
			bucket_in_prefix = True
		if bucket_in_prefix:
			config = None
			bucket = splitted_prefix[0]
			prefix = '/'.join(splitted_prefix[1:])
			s3_client = boto3.client(
			    "s3",
			    endpoint_url=f"{parsed_url.scheme}://{parsed_url.netloc}",
			    verify=verify)
			try:
				s3_client.head_bucket(Bucket=bucket)
			except ParamValidationError:
				bucket_in_prefix = False
			except ClientError:
				bucket_in_prefix = False
			bucket_name = bucket

		if not bucket_in_endpoint and not bucket_in_prefix:
			raise Exception("No valid bucket name in endpoint or prefix")

		self._error_occurred = Event()
		self.data_dir = args.data_dir
		self.bucket_name = bucket_name
		self.prefix = prefix
		self.verbose = args.verbose
		self.s3 = s3_client

	def run(self):
		chkp_num = self.last_checkpoint_number(self.bucket_name)
		self.download_files_in_directory(self.bucket_name,
		                                 'data/',
		                                 chkp_num,
		                                 self.data_dir,
		                                 transform=self.transform_pg)
		self.download_files_in_directory(self.bucket_name,
		                                 'orioledb_data/',
		                                 chkp_num,
		                                 f"{self.data_dir}/orioledb_data",
		                                 transform=self.transform_orioledb,
		                                 filter=self.filter_orioledb)

		self.download_unchanged_files(
		    self.bucket_name, os.path.join("orioledb_data", "pg_files.crc"),
		    chkp_num, None)

		control = get_control_data(self.data_dir)
		orioledb_control = get_orioledb_control_data(self.data_dir)
		self.download_undo(orioledb_control['undoRegularStartLocation'],
		                   orioledb_control['undoRegularEndLocation'],
		                   "orioledb_data/%02X%08Xdata")
		self.download_undo(orioledb_control['undoSystemStartLocation'],
		                   orioledb_control['undoSystemEndLocation'],
		                   "orioledb_data/%02X%08Xsystem")
		wal_file = control["Latest checkpoint's REDO WAL file"]
		local_path = os.path.join(self.data_dir, f"pg_wal/{wal_file}")
		wal_file = os.path.join(self.prefix, f"wal/{wal_file}")
		self.download_file(self.bucket_name, wal_file, local_path)

	def download_undo(self, startLocation, endLocation, template):
		UNDO_FILE_SIZE = 0x4000000
		if startLocation >= endLocation:
			return
		for fileNum in range(startLocation // UNDO_FILE_SIZE,
		                     (endLocation - 1) // UNDO_FILE_SIZE):
			fileName = template % (fileNum >> 32, fileNum & 0xFFFFFFFF)
			fileName = os.path.join(self.prefix, fileName)
			loader.download_file(self.bucket_name, fileName, fileName)

	def last_checkpoint_number(self, bucket_name):
		paginator = self.s3.get_paginator('list_objects_v2')

		numbers = []
		prefix = os.path.join(self.prefix, 'data/')
		for page in paginator.paginate(Bucket=bucket_name,
		                               Prefix=prefix,
		                               Delimiter='/'):
			if 'CommonPrefixes' in page:
				for prefix in page['CommonPrefixes']:
					prefix_key = prefix['Prefix'].rstrip('/')
					subdirectory = prefix_key.split('/')[-1]
					try:
						number = int(subdirectory)
						numbers += [number]
					except ValueError:
						pass

		numbers = sorted(numbers)

		found = False
		chkp_list_index = len(numbers) - 1

		last_chkp_data_dir = os.path.join(self.prefix, 'data',
		                                  str(numbers[chkp_list_index]))

		while not found and chkp_list_index >= 0:
			try:
				self.s3.head_object(
				    Bucket=bucket_name,
				    Key=f'{last_chkp_data_dir}/global/pg_control')
				self.s3.head_object(
				    Bucket=bucket_name,
				    Key=f'{last_chkp_data_dir}/orioledb_data/control')
				found = True
			except ClientError as e:
				if e.response['Error']['Code'] == "404":
					chkp_list_index -= 1
					if chkp_list_index >= 0:
						last_chkp_data_dir = os.path.join(
						    self.prefix, 'data', str(numbers[chkp_list_index]))
				else:
					raise

		if chkp_list_index < 0:
			raise Exception("Failed to find valid checkpoint in s3 bucket")

		return numbers[chkp_list_index]

	def list_objects(self, bucket_name, directory):
		objects = []
		paginator = self.s3.get_paginator('list_objects_v2')

		prefix = os.path.join(self.prefix, directory)
		for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
			if 'Contents' in page:
				page_objs = [x["Key"] for x in page['Contents']]
				objects.extend(page_objs)

		return objects

	# Reimplement os.dirs so it sets mode for intermediate dirs also
	def makedirs(self, name, mode=0o777, exist_ok=False):
		"""makedirs(name [, mode=0o777][, exist_ok=False])

		Super-mkdir; create a leaf directory and all intermediate ones.  Works like
		mkdir, except that any intermediate path segment (not just the rightmost)
		will be created if it does not exist. If the target directory already
		exists, raise an OSError if exist_ok is False. Otherwise no exception is
		raised.  This is recursive.

		"""
		head, tail = os.path.split(name)
		if not tail:
			head, tail = os.path.split(head)
		if head and tail and not os.path.exists(head):
			try:
				self.makedirs(head, mode, exist_ok=exist_ok)
			except FileExistsError:
				# Defeats race condition when another thread created the path
				pass
			cdir = os.curdir
			if isinstance(tail, bytes):
				cdir = bytes(os.curdir, 'ASCII')
			if tail == cdir:  # xxx/newdir/. exists if xxx/newdir exists
				return
		try:
			os.mkdir(name, mode)
		except OSError:
			# Cannot rely on checking for EEXIST, since the operating system
			# could give priority to other errors like EACCES or EROFS
			if not exist_ok or not os.path.isdir(name):
				raise

	def download_file(self, bucket_name, file_key, local_path):
		try:
			transfer_config = TransferConfig(use_threads=False,
			                                 max_concurrency=1)
			if file_key[-1] == '/':
				dirs = local_path
			else:
				dirs = '/'.join(local_path.split('/')[:-1])
			self.makedirs(dirs, exist_ok=True, mode=0o700)
			if file_key[-1] != '/':
				self.s3.download_file(bucket_name,
				                      file_key,
				                      local_path,
				                      Config=transfer_config)
			if self.verbose:
				print(f"{file_key} -> {local_path}", flush=True)
			if re.match(r'.*/orioledb_data/small_files_\d+$', local_path):
				base_dir = '/'.join(local_path.split('/')[:-2])
				with open(local_path, 'rb') as file:
					data = file.read()
				numFiles = struct.unpack('i', data[0:4])[0]
				for i in range(0, numFiles):
					(nameOffset, dataOffset,
					 dataLength) = struct.unpack('iii',
					                             data[4 + i * 12:16 + i * 12])
					name = data[nameOffset:data.find(b'\0', nameOffset
					                                 )].decode('ascii')
					fullname = f"{base_dir}/{name}"
					if self.verbose:
						print(f"{file_key} -> {fullname}", flush=True)
					self.makedirs(os.path.dirname(fullname),
					              exist_ok=True,
					              mode=0o700)
					with open(fullname, 'wb') as file:
						file.write(data[dataOffset:dataOffset + dataLength])
					os.chmod(fullname, 0o600)
				os.unlink(local_path)

		except ClientError as e:
			if e.response['Error']['Code'] == "404":
				print(f"File not found: {file_key}")
			else:
				print(f"An error occurred: {e}")
			self._error_occurred.set()

	def transform_orioledb(self, val: str) -> str:
		offset = 0
		prefix = self.prefix.strip('/')
		if prefix != "":
			offset = len(prefix.split('/'))
		parts = val.split('/')
		file_parts = parts[offset + 3].split('.')
		result = f"{parts[offset + 2]}/{file_parts[0]}-{parts[offset + 1]}"
		if file_parts[-1] == 'map':
			result += '.map'
		return result

	def filter_orioledb(self, val: str) -> bool:
		offset = 0
		prefix = self.prefix.strip('/')
		if prefix != "":
			offset = len(prefix.split('/'))
		parts = val.split('/')
		file_parts = parts[offset + 3].split('.')
		is_map = file_parts[-1] == 'map'
		return is_map

	def transform_pg(self, val: str) -> str:
		offset = 0
		prefix = self.prefix.strip('/')
		if prefix != "":
			offset = len(prefix.split('/'))
		parts = val.split('/')
		result = '/'.join(parts[offset + 2:])
		return result

	def download_files_in_directory(self,
	                                bucket_name,
	                                directory,
	                                chkp_num,
	                                local_directory,
	                                transform: Callable[[str], str],
	                                filter: Callable[[str], bool] = None):
		last_chkp_dir = os.path.join(directory, str(chkp_num))
		objects = self.list_objects(bucket_name, last_chkp_dir)
		max_threads = os.cpu_count()

		with ThreadPoolExecutor(max_threads) as executor:
			futures = []

			for file_key in objects:
				local_file = transform(file_key)
				if filter and not filter(file_key):
					continue
				local_path = f"{local_directory}/{local_file}"
				future = executor.submit(self.download_file, bucket_name,
				                         file_key, local_path)
				futures.append(future)

			for future in futures:
				future.result()

				if self._error_occurred.is_set():
					print("An error occurred. Stopping all downloads.")
					executor.shutdown(wait=False, cancel_futures=True)
					break

	def download_unchanged_files(self, bucket_name: str, pgfiles_name: str,
	                             chkp_num: int,
	                             pgfiles: list[dict[str, str]] | None):
		# We won't be able to download unchanged previous files if this is
		# the first checkpoint
		if chkp_num <= 1:
			return

		prev_chkp_num = chkp_num - 1
		prev_chkp_dir = os.path.join(self.prefix, "data", str(prev_chkp_num))

		if pgfiles is None:
			pgfiles_path = os.path.join(self.data_dir, pgfiles_name)
			pgfiles = self.get_pgfiles_hash(pgfiles_path)

		prev_pgfiles = []
		for pgfile in pgfiles:
			# Ignore changed files
			if int(pgfile["checkpoint"]) == chkp_num:
				continue

			if int(pgfile["checkpoint"]) > chkp_num:
				raise Exception(f"Unexpected checkpoint number {pgfile}")

			# This file needs to be downloaded from pre-previous checkpoint
			if int(pgfile["checkpoint"]) < prev_chkp_num:
				prev_pgfiles.append(pgfile)
				continue

			remote_file = os.path.join(prev_chkp_dir, pgfile["filename"])
			local_file = os.path.join(self.data_dir, pgfile["filename"])

			self.download_file(bucket_name, remote_file, local_file)

		# Some files are still missing
		if len(prev_pgfiles) > 0:
			remote_pgfiles_path = os.path.join(prev_chkp_dir, pgfiles_name)
			temp_pgfiles_path = os.path.join(
			    self.data_dir, f"{pgfiles_name}.{prev_chkp_num}")

			self.download_file(bucket_name, remote_pgfiles_path,
			                   temp_pgfiles_path)

			# Recursively download unchanged files
			self.download_unchanged_files(bucket_name, pgfiles_name,
			                              prev_chkp_num, prev_pgfiles)

			os.unlink(temp_pgfiles_path)

	def get_pgfiles_hash(self, pgfiles_hash_name: str):
		res = []

		pattern_str = r"^FILE: (?P<filename>.+), HASH: (?P<hash>.+), CHECKPOINT: (?P<checkpoint>\d+)$"
		pattern = re.compile(pattern_str)
		with open(pgfiles_hash_name) as file:
			for line in file:
				m = pattern.search(line)

				if m is None or len(m.groups()) != 3:
					raise Exception(
					    f"Invalid line format of the hash file {pgfiles_hash_name}: {line}"
					)

				res.append(m.groupdict())

		return res


def get_control_data(data_dir: str):
	"""
	Return contents of pg_control file.
	"""

	# this one is tricky (blame PG 9.4)
	_params = [testgres.get_bin_path("pg_controldata")]
	_params += ["-D"]
	_params += [data_dir]

	data = testgres.utils.execute_utility(_params)

	out_dict = {}

	for line in data.splitlines():
		key, _, value = line.partition(':')
		out_dict[key.strip()] = value.strip()

	return out_dict


def get_orioledb_control_data(data_dir: str):
	"""
	Return contents of OrioleDB control file.
	"""

	f = open(f"{data_dir}/orioledb_data/control", 'rb')
	data = f.read(8 * 13)
	(undoRegularStartLocation,
	 undoRegularEndLocation) = struct.unpack('QQ', data[8 * 8:8 * 10])
	(undoSystemStartLocation,
	 undoSystemEndLocation) = struct.unpack('QQ', data[8 * 11:8 * 13])
	f.close()

	dict = {
	    'undoRegularStartLocation': undoRegularStartLocation,
	    'undoRegularEndLocation': undoRegularEndLocation,
	    'undoSystemStartLocation': undoSystemStartLocation,
	    'undoSystemEndLocation': undoSystemEndLocation
	}

	return dict


if __name__ == '__main__':
	loader = OrioledbS3ObjectLoader()
	loader.parse_args()
	loader.run()
