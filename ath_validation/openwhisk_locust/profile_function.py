# test
# python3 ./profile_function.py --min-users 1 --max-users 2 --user-step 1 --exp-time 60s --profile-users 1 --profile-time 60s --warmup-time 30s --function mobilenet 
# python3 ./profile_function.py --min-users 5 --max-users 30 --user-step 5 --profile-users 10 --function mobilenet

# assume docker version >= 1.13
import sys
import os
import time
import numpy as np
import json
import math
import random
import argparse
import logging
import subprocess
from pathlib import Path
import copy
import shutil

from pathlib import Path
sys.path.append(str(Path.cwd() / 'util'))
from db_activation import *

# from socket import SOCK_STREAM, socket, AF_INET, SOL_SOCKET, SO_REUSEADDR

random.seed(time.time())
# -----------------------------------------------------------------------
# miscs
# -----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
					format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser()
# parser.add_argument('--cpus', dest='cpus', type=int, required=True)
# parser.add_argument('--stack-name', dest='stack_name', type=str, required=True)
parser.add_argument('--function', dest='function', type=str, required=True)
parser.add_argument('--min-users', dest='min_users', type=int, required=True)
parser.add_argument('--max-users', dest='max_users', type=int, required=True)
parser.add_argument('--user-step', dest='user_step', type=int, required=True)
parser.add_argument('--exp-time', dest='exp_time', type=str, default='5m')
parser.add_argument('--warmup-time', dest='warmup_time', type=str, default='1m')
parser.add_argument('--profile-users', dest='profile_users', type=int, required=True)
parser.add_argument('--profile-time', dest='profile_time', type=str, default='20m')
args = parser.parse_args()

function = args.function
min_users = args.min_users
max_users = args.max_users
user_step = args.user_step
exp_time = args.exp_time
warmup_time = args.warmup_time
profile_users = args.profile_users
profile_time = args.profile_time

data_dir = Path.cwd() / 'data'
distr_data_dir = Path.cwd() / 'data' / 'distr'
locust_stats_dir = Path.home() / 'openwhisk_locust_log'

# openwhisk
openwhisk_controller_log = Path('/tmp/wsklogs/controller0/controller0_logs.log')

if not os.path.isdir(str(data_dir)):
	os.makedirs(str(data_dir))

if not os.path.isdir(str(distr_data_dir)):
	os.makedirs(str(distr_data_dir))

script = Path.cwd() / 'scripts' / ('test_action.sh')
assert os.path.isfile(str(script))

tested_users = range(min_users, max_users+1, user_step)
print('users')
print(tested_users)

def change_time(time_str):
	if 'm' in time_str:
		return int(time_str.replace('m', '')) * 60
	elif 's' in time_str:
		return int(time_str.replace('s', ''))
	else:
		return int(time_str)

def run_mpstat(test_time, file_handle):
	cmd = 'mpstat -P ALL 1 ' + str(change_time(test_time))
	print(cmd)
	p = subprocess.Popen(cmd, shell=True, stdout=file_handle)
	return p

def run_exp(test_time, user, quiet=False):
	cmd = str(script) + ' ' + str(test_time) + ' ' + str(user) + ' ' + function
	if not quiet:
		p = subprocess.Popen(cmd, shell=True)
	else:
		p = subprocess.Popen(cmd, shell=True, 
			stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
	return p

def copy_locust_stats(dir_name):
	full_path = data_dir / dir_name
	shutil.copytree(str(locust_stats_dir), str(full_path))

def get_activation_ids():
	full_path = locust_stats_dir / 'locust_openwhisk_log.txt'
	aids = {}	# indexed by function name
	with open(str(full_path), 'r') as f:
		lines = f.readlines()
		for line in lines:
			if 'aid--' in line:
				data = line.split('aid--')[-1]
				action, aid = data.split(':')
				if action not in aids:
					aids[action] = []
				aids[action] += [aid]
	return aids

def clear_locust_state():
	for fn in os.listdir(str(locust_stats_dir)):
		full_path = locust_stats_dir / fn
		os.remove(str(full_path))

def controller_log_length():
	l = 0
	with open(str(openwhisk_controller_log), 'r') as f:
		lines = f.readlines()
		l = len(lines)
	return l

def grep_function_distr(tail_len, distr_file):
	chosen = []
	with open(str(openwhisk_controller_log), 'r') as f:
		lines = f.readlines()[-tail_len:]
		for line in lines:
			if 'exe time' in line:
				chosen.append(line)

	distr_file_path = str(distr_data_dir / distr_file)
	with open(distr_file_path, 'w+') as f:
		for l in chosen:
			f.write(l + '\n')

# # check log
# log_init_length = controller_log_length()
# print('log_init_length = %d' %log_init_length)
# # profile function distr
# p = run_exp(test_time=profile_time, user=profile_users)
# p.wait()
# time.sleep(120)
# log_length = controller_log_length()
# print('log_length = %d' %log_length)

# distr_file = function + '_distr.txt'
# grep_function_distr(tail_len=log_length-log_init_length, distr_file=distr_file)

time.sleep(10)
# stress test
for u in tested_users:
	# warumup
	p = run_exp(test_time=warmup_time, user=u)
	p.wait()
	# time.sleep(10)
	# real exp
	mpstat_file = str(data_dir / ('mpstat_' + function + '_user_' + str(u) + '.txt'))
	f = open(mpstat_file, 'w+')
	pm = run_mpstat(test_time=exp_time, file_handle=f)
	pl = run_exp(test_time=exp_time, user=u)

	pl.wait()
	pm.wait()
	f.flush()
	f.close()

	# read activation data from db
	aids = get_activation_ids()

	for action in aids:
		print('action %s' %action)
		for aid in aids[action]:
			print(aid)
			while True:
				doc = get_activation_by_id()
				if doc == None:
					print('wait for couchdb...')
					time.sleep(5)
				else:
					print(doc)
					break

	dir_name = 'locust_' + function + '_user_' + str(u)
	copy_locust_stats(dir_name)
	clear_locust_state()
	time.sleep(10)

