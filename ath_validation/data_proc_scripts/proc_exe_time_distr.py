import sys
import os
from pathlib import Path
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('--source-path', dest='source_path', type=str, required=True)
parser.add_argument('--target-path', dest='target_path', type=str, required=True)
args = parser.parse_args()

source_path = Path(args.source_path)
target_path = Path(args.target_path)

max_time = 60*1000	# in ms
cpu_usage_distr = {}	# in percent
exe_time_distr = {}	# in ms
overhead_distr = {}	# in ms

class Distribution:
	def __init__(self):
		self.min = 0
		self.max = 0
		self.samples = 0
		self.distr = {}

	def update(self, val):
		assert val >= 0
		if val not in self:
			self.min = val
			self.max = val
			self.distr[val] = 0
		self.min = min(self.min, val)
		self.max = max(self.max, val)
		self.distr[val] += 1
		self.samples += 1

	def save_json(self, path):
		if len(self.distr) == 0:
			return
		with open(str(path), 'w+') as f:
			contents = {}
			contents['min'] = self.min
			contents['max'] = self.max
			contents['samples'] = self.samples
			contents['distribution'] = self.distr
			for val in contents['distributions']:
				contents['distributions'][val] = round(contents['distributions'][val] / samples, 5)
			json.dump(contents, f, indent=4, sort_keys=True)

def proc_log(file):
	global max_time
	global exe_time_distr
	global overhead_distr
	global cpu_usage_distr

	with open(str(file), 'r') as f:
		lines = f.readlines()
	for line in lines:
		if 'function guest/' in line:
			func, rest = line.split('function guest/')[-1].split('@')
			items = rest.split(',')

			func = func.replace('/', '--')

			if func not in exe_time_distr:
				exe_time_distr[func] = Distribution()
				overhead_distr[func] = Distribution()
				cpu_usage_distr[func] = Distribution()

			cpu_usage = -1
			exe_time = -1
			total_time = -1
			overhead = -1

			for item in items:
				# ms precision
				if 'cpu usage' in item:
					cpu_usage = int(float(item.split('cpu usage')[-1].replace('=', '')) * 100)
				elif 'exe time' in item:
					exe_time = int(item.split('exe time')[-1].replace('=', '')) // 1000
				elif 'total time' in item:
					total_time = int(item.split('total time')[-1].replace('=', '')) // 1000
					assert exe_time >= 0
					overhead = total_time - exe_time

			if cpu_usage < 0 or exe_time < 0 or cpu_usage > 100:
				print(line)
			assert cpu_usage >= 0
			assert exe_time >= 0
			# assert total_time >= 0

			if exe_time > 0 and cpu_usage > 0:
				# meaningful info
				exe_time_distr[func].update(exe_time)
				cpu_usage_distr[func].update(cpu_usage)
				if overhead >= 0:
					overhead_distr[func].update(overhead)


if __name__ == '__main__':
	proc_log(source_path)
	for func in exe_time_distr:
		exe_time_distr[func].save_json(target_path / ('exe_time_'+str(func)+'.json'))
		cpu_usage_distr[func].save_json(target_path / ('cpu_usage_'+str(func)+'.json'))
		overhead_distr[func].save_json(target_path / ('overhead_'+str(func)+'.json'))

