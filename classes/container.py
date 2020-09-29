import lxc
import docker
import psutil
import sys
import os
import stat
import shutil
import logging
import subprocess
import mmap
from datetime import datetime
from configparser import ConfigParser


# Master class containing basic functions for containers


class Container:


	# Constructor from container class


	def __init__(self, name='', cid=int, template='', command='', min_mem_limit=int, max_mem_limit=int, request_cpus=int, apptype=''):
		self.name = name
		self.cid = cid
		self.template = template
		self.command = command
		self.min_mem_limit = min_mem_limit
		self.max_mem_limit = max_mem_limit
		self.request_cpus = request_cpus
		self.apptype = apptype
		self.state = 'NEW'
		self.ip = ''
		# CPU Set and Info
		self.cpu_set = ''
		self.cpu_used = 0
		self.lxc_state = ''
		# Memory Info
		self.mem_limit = -1
		self.mem_faults = 0
		self.mem_swap_limit = -1
		self.mem_swap_faults = 0
		self.mem_stats = {}
		self.mem_swappiness = 0
		# Scheduler info
		self.mem_state = ''
		self.mem_delta = 0
		self.mem_delta_check = False
		self.mem_steal = 0
		self.mem_steal_check = False
		self.block_provider = False
		self.update_LRU = False
		self.block_repo = False
		self.mem_repo = False
		self.repo_SI = False
		self.mem_used_pre_repo = 0
		self.last_repo = 0
		self.repo_lim = min_mem_limit
		self.last_data_analyzed = 0
		# Disk Access info
		self.disk_stats = {}
		self.disk_recursive_stats = {}
		self.disk_th_stats = {}
		self.disk_th_recursive_stats = {}
		# Time Info
		self.start_time = 0
		self.mem_state_time = 0
		self.inactive_time = 0
		self.estimated_time = 0
		# Process Info
		self.pid = 0
		self.process_command = ''
		self.process_status = ''
		self.process_memory_info = ''
		self.process_cpu_used = 0
		self.children = []
		self.children_stats = []
		# Host Info
		self.host_memory_stats = ''
		self.host_swap_stats = ''


	# Function to verify an equality of two containers


	def __eq__(self, other):
		if isinstance(other, self.__class__):

			if self.name == other.name:
				return True

			else:
				return False


	# Functions to print containers


	def __str__(self):
		out = []

		for key in self.__dict__:
			out.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

		return ', '.join(out)


	def __repr__(self):
		return self.name + ' ' + self.state


	def printResume(self):
		print('Container:')
		print('Name: ', self.name)
		print('Image: ', self.template)
		print('Command: ', self.command)
		print('Request Memory (Initial Limit): ', self.request_mem)
		print('Number of Request CPU: ', self.request_cpus)


	# Function to calculate the running time from a container


	def getRunningTime(self):
		time_lapse = datetime.now() - self.start_time
		#print('Running Time (in seconds): ', int(time_lapse.total_seconds()))
		return time_lapse.total_seconds()


	# Function to calculate the inactive time from a container


	def getInactiveTime(self):
		time_lapse = datetime.now() - self.inactive_time
		#print('Inactive Time (in seconds): ', int(time_lapse.total_seconds()))
		return time_lapse.total_seconds()


	# Function to calculate the remaining time from a container, based on the estimated time


	def getRemainingTime(self):
		runtime = datetime.now() - self.start_time
		remaintime = self.estimated_time - runtime
		#print('Remaining Time (in seconds): ', int(remaintime.total_seconds()))
		return remaintime


	# Function to calculate the time since last container memory state change


	def getMemoryStateTime(self):
		time_lapse = datetime.now() - self.mem_state_time
		#print('Memory State Time (in seconds):', int(time_lapse.total_seconds()))
		return time_lapse.total_seconds()


	# Functions to calculate container memory usage


	def getUsedMemory(self):
		result = int(self.mem_stats['rss']) + int(self.mem_stats['cache'])
		#result = int(self.mem_stats.get('rss')) + int(self.mem_stats.get('cache'))
		return result


	def getUsedMemory2(self):
		result = int(self.mem_stats['rss']) + int(self.mem_stats['cache']) + int(self.mem_stats['swap'])
		return result


	def getUsedMemoryPG(self):
		result = round((int(self.mem_stats['rss']) + int(self.mem_stats['cache'])) / mmap.PAGESIZE)
		return result


	def getUsedSwap(self):
		return int(self.mem_stats['swap'])


	def getUsedSwapPG(self):
		result = round(int(self.mem_stats['swap']) / mmap.PAGESIZE)
		return result


	def getInactiveMemory(self):
		return int(self.mem_stats['inactive_anon'])


	def getInactiveMemoryPG(self):
		return round(int(self.mem_stats['inactive_anon']) / mmap.PAGESIZE)


	# Functions to get Container Limits


	def getMemoryLimit(self):
		return self.mem_limit


	def getMemoryLimitPG(self):
		return round(self.mem_limit / mmap.PAGESIZE)


	def getSwapLimit(self):
		return self.mem_swap_limit


	def getMinMemoryLimit(self):
		return self.min_mem_limit


	def getMinMemoryLimitPG(self):
		return round(self.min_mem_limit / mmap.PAGESIZE)


	def getMaxMemoryLimit(self):
		return self.max_mem_limit


	def getMaxMemoryLimitPG(self):
		return round(self.max_mem_limit / mmap.PAGESIZE)


	def getSwappiness(self):
		return self.mem_swappiness


	# Functions to get and set Container Delta temporary values


	def getDeltaMemory(self):
		return self.mem_delta


	def setDeltaMemory(self, delta:int):
		self.mem_delta = delta


	# Function to calculate the memory threshold of a Container
	# The threshold is a relationship between the memory usage and the memory limit of a Container, in percentage


	def getMemoryThreshold(self):
		result = ((int(self.mem_stats['rss']) + int(self.mem_stats['cache'])) * 100) // self.mem_limit
		return result


	# Funtion to get the page major faults from a Container


	def getMemoryPageFaults(self):
		return int(self.mem_stats['pgfault'])


	def getMemoryMajorFaults(self):
		return int(self.mem_stats['pgmajfault'])


	# Function to get and set Memory State


	def setMemoryState(self, state):
		self.mem_state = state


	def getMemoryState(self):
		return self.mem_state


	#Function to get and set Container states


	def setContainerState(self, state):
		self.state = state


	def getContainerState(self):
		return self.state


	def getHostInfo(self):
		self.host_memory_stats = psutil.virtual_memory()
		self.host_swap_stats = psutil.swap_memory()


	def getCpuset(self):
		return self.cpu_set.split()



# Children class from Container containing functions to manager lxc containers


class ContainerLXC(Container):

	def updateState(self):
		container = lxc.Container(self.name)

		try:
			if container.defined:
				self.lxc_state = container.state
				#print('LXC:', self.lxc_state, 'VEMoC:', self.state)

				if self.lxc_state == 'FROZEN':
					self.state = 'PAUSED'

				elif self.lxc_state == 'RUNNING' and self.state != 'SUSPENDING':
					self.state = 'RUNNING'

				elif self.lxc_state == 'STOPPED':

					if self.state == 'SUSPENDING':
						self.state = 'SUSPENDED'

					elif self.state not in ['QUEUED','SUSPENDING','SUSPENDED','RESUMING','FINISHED']:
						self.state = 'FINISHED'

				logging.info('Container: %s - VEMoC_State: %s - LXC_State: %s', self.name, self.state, self.lxc_state)

		except Exception as err:
			logging.error('Fail to update container %s state', self.name)
			logging.error('Error: %s', err)
			pass


	def update(self):
		container = lxc.Container(self.name)

		if container.defined:
			logging.debug('Updating container %s info with status %s and state %s', container.name, container.state, self.state)

			for x in range(3):
				str_error = None
				logging.info('Container %s Update Try %d', container.name, (x+1))

				try:
					#Host stats
					self.host_memory_stats = psutil.virtual_memory()
					self.host_swap_stats = psutil.swap_memory()

					if (container.state != 'STOPPED'):
						if container.state == 'FROZEN':
							self.state = 'PAUSED'
						elif container.state == 'RUNNING' and self.state != 'SUSPENDING':
							self.state = container.state

						if self.state not in ['SUSPENDING','SUSPENDED','FINISHED','RESUMING']:
							self.ip = container.get_ips()
							self.cpu_set = container.get_cgroup_item('cpuset.cpus')
							self.cpu_used = int(container.get_cgroup_item('cpuacct.usage'))
							self.mem_limit = int(container.get_cgroup_item('memory.limit_in_bytes'))
							self.mem_faults = int(container.get_cgroup_item('memory.failcnt'))
							self.mem_swap_limit = int(container.get_cgroup_item('memory.memsw.limit_in_bytes'))
							self.mem_swap_faults = int(container.get_cgroup_item('memory.memsw.failcnt'))
							self.mem_soft_limit = int(container.get_cgroup_item('memory.soft_limit_in_bytes'))
							self.mem_swappiness = int(container.get_cgroup_item('memory.swappiness'))
							temp = container.get_cgroup_item('memory.stat')
							self.mem_stats = dict(item.split(" ") for item in temp.split("\n"))
							#temp = container.get_cgroup_item('blkio.io_service_bytes')
							#self.disk_stats = dict(item.split(" ") for item in temp.split("\n"))
							#temp = container.get_cgroup_item('blkio.io_service_bytes_recursive')
							#self.disk_recursive_stats = dict(item.split(" ") for item in temp.split("\n"))
							#temp = container.get_cgroup_item('blkio.throttle.io_service_bytes')
							#self.disk_th_stats = dict(item.split(" ") for item in temp.split("\n"))
							#temp = container.get_cgroup_item('blkio.throttle.io_service_bytes_recursive')
							#self.disk_th_recursive_stats = dict(item.split(" ") for item in temp.split("\n"))

							self.pid = container.init_pid

							if psutil.pid_exists(self.pid):
								proc = psutil.Process(self.pid)
								self.children = proc.children(recursive = True)

								with proc.oneshot():
									self.process_status = proc.status()
									self.process_command = proc.cmdline()
									self.process_memory_info = proc.memory_full_info()
									self.process_cpu_used = proc.cpu_times()

								self.children_stats = []

								for p in self.children:
									temp2 = {}

									if psutil.pid_exists(p.pid):
										with p.oneshot():
											temp2['cpu_used'] = p.cpu_times()
											temp2['memory_used'] = p.memory_full_info()
											temp2['status'] = p.status()
											temp2['command'] = p.cmdline()
											temp2['pid'] = p.pid
											temp2['parent'] = p.ppid()

										self.children_stats.append(temp2)

							#Host stats
							#self.host_memory_stats = psutil.virtual_memory()
							#self.host_swap_stats = psutil.swap_memory()

					else:

						if self.state == 'SUSPENDING':
							self.state = 'SUSPENDED'

						elif self.state not in ['RESUMING','SUSPENDING','SUSPENDED','QUEUED','PAUSED','FINISHED']:
							self.state = 'FINISHED'

					#str_error = None

				except Exception as err:
					logging.error('Fail to update container %s stats - Try %d', self.name, (x+1))
					#exc_type, exc_obj, exc_tb = sys.exc_info()
					#logging.error('Error: %s Line: %s', exc_type, exc_tb.tb_lineno)
					str_error = err
					logging.error('Error: %s', str_error)
					pass

				if not str_error:
					break


	def update2(self):
		container = lxc.Container(self.name)

		if container.defined:
			logging.debug('Updating container %s info with state %s', container.name, self.state)

			for x in range(3):
				str_error = None
				logging.info('Container %s Update Try %d', container.name, (x+1))

				try:
					if self.state in ['RUNNING','PAUSED']:
						logging.info('Get Info from LXC for %s', container.name)
						self.ip = container.get_ips()
						self.cpu_set = container.get_cgroup_item('cpuset.cpus')
						self.cpu_used = int(container.get_cgroup_item('cpuacct.usage'))

						self.mem_limit = int(container.get_cgroup_item('memory.limit_in_bytes'))
						self.mem_faults = int(container.get_cgroup_item('memory.failcnt'))
						self.mem_swap_limit = int(container.get_cgroup_item('memory.memsw.limit_in_bytes'))
						self.mem_swap_faults = int(container.get_cgroup_item('memory.memsw.failcnt'))
						self.mem_swappiness = int(container.get_cgroup_item('memory.swappiness'))
						temp = container.get_cgroup_item('memory.stat')
						self.mem_stats = dict(item.split(" ") for item in temp.split("\n"))

						#temp = container.get_cgroup_item('blkio.io_service_bytes')
						#self.disk_stats = dict(item.split(" ") for item in temp.split("\n"))
						#temp = container.get_cgroup_item('blkio.io_service_bytes_recursive')
						#self.disk_recursive_stats = dict(item.split(" ") for item in temp.split("\n"))
						#temp = container.get_cgroup_item('blkio.throttle.io_service_bytes')
						#self.disk_th_stats = dict(item.split(" ") for item in temp.split("\n"))
						#temp = container.get_cgroup_item('blkio.throttle.io_service_bytes_recursive')
						#self.disk_th_recursive_stats = dict(item.split(" ") for item in temp.split("\n"))

						self.pid = container.init_pid

						if psutil.pid_exists(self.pid):
							proc = psutil.Process(self.pid)
							self.children = proc.children(recursive = True)

							with proc.oneshot():
								self.process_status = proc.status()
								self.process_command = proc.cmdline()
								self.process_memory_info = proc.memory_full_info()
								self.process_cpu_used = proc.cpu_times()

							self.children_stats = []

							for p in self.children:
								temp2 = {}

								if psutil.pid_exists(p.pid):
									with p.oneshot():
										temp2['cpu_used'] = p.cpu_times()
										temp2['memory_used'] = p.memory_full_info()
										temp2['status'] = p.status()
										temp2['command'] = p.cmdline()
										temp2['pid'] = p.pid
										temp2['parent'] = p.ppid()

									self.children_stats.append(temp2)

				except Exception as err:
					logging.error('Fail to update container %s stats - Try %d', self.name, (x+1))
					str_error = err
					logging.error('Error: %s', str_error)
					pass

				if not str_error:
					break


	# Método para verificar se um container existe


	def checkContainer(self):
		try:
			container = lxc.Container(self.name)
			check = container.defined
			#print('Container Exists? ', check, flush=True)
			return check

		except Exception as err:
			logging.error('Fail to update container %s stats', self.name)
			exc_type, exc_obj, exc_tb = sys.exc_info()
			logging.error('Error: %s Line: %s', exc_type, exc_tb.tb_lineno)
			logging.error('Error: %s', err)
			return False


	# Método para criar um container


	def createContainer(self):
		container = lxc.Container(self.name)

		config = ConfigParser()
		config.read('./config/local-config.txt')
		template_type = config['Template']['type']
		template_path = config['Template']['path']

		logging.info('Creating Container %s', self.name)

		try:
			if template_type == 'oci':
				url = 'oci:' + template_path + '/' + self.template
				if not container.create(template=template_type, args={'url': url}):
					logging.error('Fail to Create the Container %s', self.name)

			elif template_type == 'default':
				if not container.create(self.template):
					logging.error('Fail to Create the Container %s', self.name)

			container.wait('STOPPED', timeout=30)
			container.append_config_item(key='lxc.cgroup.relative', value='1')
			container.append_config_item(key='lxc.include', value='/usr/share/lxc/config/checkpoint.conf')
			container.save_config()
			self.state = 'CREATED'
			logging.info('Container %s Created with Success', self.name)

		except Exception as err:
			logging.error('Fail to Create Container %s with error: %s', self.name, err)


	# Método para remover um container


	def destroyContainer(self):
		container = lxc.Container(self.name)
		logging.info('Destroying Container %s', self.name)

		if container.defined:
			try:
				if not container.destroy():
					logging.error('Fail to Destroy the Container %s', self.name)

				logging.info('Container %s was Destroyed', self.name)

			except Exception as err:
				logging.error('Fail to Destroy Container %s with Error: %s', self.name, err)


	# Método para a inicialização de um container existente


	def startContainer(self):
		container = lxc.Container(self.name)

		logging.info('Starting Container %s', self.name)

		try:
			container.start(cmd =tuple(self.command.split()))
			container.wait('RUNNING', timeout=60)
			self.start_time = datetime.now()
			logging.info('Container %s Started with Success', self.name)

		except Exception as err:
			logging.error('Fail to Start the Container %s with Error: %s', self.name, err)


	# Método para parar um container em execução


	def stopContainer(self):
		container = lxc.Container(self.name)
		logging.info('Stopping Container %s', self.name)

		try:
			if not container.stop():
				logging.error('Fail to Stop the Container %s', self.name)

			container.wait('STOPPED', timeout=60)
			logging.info('Container %s Stopped with Success', self.name)

		except Exception as err:
			logging.error('Fail to Stop the Container %s with Error: %s', self.name, err)


	# Método para congelar um container em execução


	def pauseContainer(self):
		container = lxc.Container(self.name)
		logging.info('Pausing Container %s', self.name)

		try:
			if not container.freeze():
				logging.error('Fail to Freeze the Container %s', self.name)

			container.wait('FROZEN', timeout=60)
			self.state == 'PAUSED'
			logging.info('Container %s Paused with Success', self.name)

		except Exception as err:
			logging.error('Fail to Pause the Container %s with Error: %s', self.name, err)


	# Método para descongelar um container em execução


	def unpauseContainer(self):
		container = lxc.Container(self.name)
		logging.info('Unpausing Container %s', self.name)

		try:
			if not container.unfreeze():
				logging.error('Fail to Unfreeze the Container %s', self.name)

			container.wait('RUNNING', timeout=60)
			logging.info('Container %s Unpaused with Success', self.name)

		except Exception as err:
			logging.error('Fail to Unpause the Container %s with Error: %s', self.name, err)


	# Método para suspender um container em execução


	def suspendContainer(self):
		config = ConfigParser()
		config.read('./config/local-config.txt')
		checkpoint_path = config['Checkpoint']['Path']
		path = checkpoint_path + '/' + self.name
		container = lxc.Container(self.name)
		logging.info('Suspending Container %s', self.name)

		try:
			if os.path.exists(path):
				shutil.rmtree(path)

			subprocess.run(['lxc-checkpoint', '-s', '-v', '-D', path, '-n', self.name,'&'])
			#command = 'lxc-checkpoint -s -v -D ' + path + ' -n ' + self.name
			#os.system(command)
			container.wait('STOPPED', timeout=60)
			self.state = 'SUSPENDED'
			logging.info('Container %s Suspended with Success', self.name)

		except Exception as err:
			logging.error('Fail to Suspended the Container %s', self.name)
			logging.error('Error: %s', err)


	def suspendContainer2(name:str):
		config = ConfigParser()
		config.read('./config/local-config.txt')
		checkpoint_path = config['Checkpoint']['Path']
		path = checkpoint_path + '/' + name
		container = lxc.Container(name)

		logging.info('Suspending Container %s', name)

		try:
			if os.path.exists(path):
				shutil.rmtree(path)

			subprocess.check_call(['lxc-checkpoint', '-s', '-v', '-D', path, '-n', name])
			container.wait('STOPPED')
			logging.info('Container %s Suspended with Success', name)

		except Exception as err:
			logging.error('Fail to Suspended the Container %s', name)
			logging.error('Error: %s', err)


	# Método para despausar um container em execução


	def resumeContainer(self, cpuset, limit):
		config = ConfigParser()
		config.read('./config/local-config.txt')
		checkpoint_path = config['Checkpoint']['Path']
		path = checkpoint_path + '/' + self.name
		container = lxc.Container(self.name)
		logging.info('Resuming Container %s', self.name)

		try:
			subprocess.run(['lxc-checkpoint', '-r', '-v', '-D', path, '-n', self.name,'&'])
			#command = 'lxc-checkpoint -r -v -D ' + path + ' -n ' + self.name
			#os.system(command)
			container.wait('RUNNING', timeout=60)
			logging.info('Container %s Resumed with Success', self.name)
			self.setMemLimit2(limit)
			self.setCPUCores(cpuset)

			if os.path.exists(path):
				shutil.rmtree(path)

		except Exception as err:
			logging.error('Fail to Resume the Container %s', self.name)
			logging.error('Error: %s', err)


	def resumeContainer2(name:str, cpuset:str):
		config = ConfigParser()
		config.read('./config/local-config.txt')
		checkpoint_path = config['Checkpoint']['Path']
		path = checkpoint_path + '/' + name
		container = lxc.Container(name)

		logging.info('Resuming Container %s', name)

		try:
			subprocess.check_call(['lxc-checkpoint', '-r', '-v', '-D', path, '-n', name])
			container.wait('RUNNING')
			logging.info('Container %s Resumed with Success', name)
			container.set_cgroup_item('cpuset.cpus', cpuset)

			if os.path.exists(path):
				shutil.rmtree(path)

		except Exception as err:
			logging.error('Fail to Resume the Container %s', name)
			logging.error('Error: %s', err)


	# Método para alocar, realocar ou remover cores de um container, usando o cgroups


	def setCPUCores(self, cores=''):
		container = lxc.Container(self.name)
		logging.info('Container %s Old Core Set: %s', self.name, self.cpu_set)
		logging.info('Container %s New Core Set: %s', self.name, cores)

		try:
			container.set_cgroup_item('cpuset.cpus', cores)
			self.cpu_set = container.get_cgroup_item('cpuset.cpus')

		except Exception as err:
			logging.error('Fail in Set the Cpu Core Affinity on the Container %s', self.name)
			logging.error('Error: %s', err)


	# Método para definir limite de uso de memória de um container, usando o cgroups


	def setMemLimit(self, limit='', swap=''):
		container = lxc.Container(self.name)
		logging.info('Container %s old limit: %d', self.name, self.mem_limit)
		logging.info('Container %s set new memory limit: %s', self.name, limit)

		try:
			container.set_cgroup_item('memory.limit_in_bytes', limit)
			container.set_cgroup_item('memory.memsw.limit_in_bytes', swap)

		except Exception as err:
			logging.error('Fail in Set Memory + Swap Usage Limit on the Container %s', self.name)
			logging.error('Error: %s', err)


	def setMemLimit2(self, limit:int):
		container = lxc.Container(self.name)
		limit = limit * mmap.PAGESIZE
		print('NEW Limit (in bytes):', limit)

		try:
			swap = psutil.swap_memory().total + limit
			logging.info('Container %s old limit: %d', self.name, self.mem_limit)
			container.set_cgroup_item('memory.limit_in_bytes', str(limit))
			container.set_cgroup_item('memory.memsw.limit_in_bytes', str(swap))
			self.mem_limit = int(container.get_cgroup_item('memory.limit_in_bytes'))
			self.mem_swap_limit = int(container.get_cgroup_item('memory.memsw.limit_in_bytes'))
			logging.info('Container %s set new memory limit: %s', self.name, self.mem_limit)
			logging.info('Container %s set new mem + swap limit: %s', self.name, self.mem_swap_limit)

		except Exception as err:
			logging.error('Fail in Set Memory + Swap Usage Limit on the Container %s', self.name)
			logging.error('Error: %s', err)


	def setSwappiness(self, swpss):
		container = lxc.Container(self.name)

		try:
			container.set_cgroup_item('memory.swappiness', str(swpss))
			self.mem_swappiness = int(container.get_cgroup_item('memory.swappiness'))
			logging.info('Container %s set swappiness to %d', self.name, self.mem_swappiness)
			print('Container ', self.name, ' set swappiness to ', self.mem_swappiness)

		except Exception as err:
			logging.error('Fail in Set Swappiness on the Container %s', self.name)
			logging.error('Error: %s', err)
			print('Err:', err)


	# Metodo para criar arquivo de um workflow


	def setWorkflow(self, command_list = []):
		container = lxc.Container(self.name)

		if container.defined:
			path_arq = container.get_config_path() + '/' + self.name + '/rootfs/opt/workflow.sh'

			try:
				arq = open(path_arq, 'w+')
				arq.write('#!/bin/bash' + '\n')

				for index, cmd in enumerate(command_list):
					if index < (len(command_list) - 1):
						arq.write(cmd + ' &\n')
					else:
						arq.write(cmd + '\n')

				arq.close()

			except IOError as err:
				logging.error('Fail in create and/or write the workflow arquive in container %s', self.name)
				logging.error(err)

			current_stat = stat.S_IMODE(os.lstat(path_arq).st_mode)

			if(current_stat != stat.S_IRWXU):
				os.chmod(path_arq, stat.S_IRWXU)
			self.command = '/opt/workflow.sh'


# Children class from Container, containing functions to manager docker containers


class ContainerDocker(Container):

	def updateState(self):
		client = docker.from_env()
		container = client.containers.get(self.name)

		try:
			self.lxc_state = container.status
				#print('LXC:', self.lxc_state, 'VEMoC:', self.state)

			if self.lxc_state == 'paused':
				self.state = 'PAUSED'

			elif self.lxc_state == 'running' and self.state != 'SUSPENDING':
				self.state = 'RUNNING'

			elif self.lxc_state == 'exited':

				if self.state == 'SUSPENDING':
					self.state = 'SUSPENDED'

				elif self.state not in ['QUEUED','SUSPENDING','SUSPENDED','RESUMING','FINISHED']:
					self.state = 'FINISHED'

		except Exception as err:
			logging.error('Fail to update container %s state', self.name)
			logging.error('Error: %s', err)
			pass


	def update(self):
		client = docker.from_env()

		try:
			container = client.containers.get(self.name)
			stats = container.stats(stream=False)
			logging.debug('Updating container  %s  info with status %s', container.name, container.status)

			self.state = self.updateStatus(container.status)

			if (self.state in ['RUNNING', 'FREEZED', 'SUSPENDED']):
				self.state = self.updateStatus(container.status)
			#	self.ip = container.get_ips()
				self.cpu_set = container.attrs['HostConfig']['CpusetCpus']
				self.cpu_used = stats['cpu_stats']['cpu_usage']
				self.mem_limit = container.attrs['HostConfig']['Memory']
				#self.mem_faults = stats['memory_stats']['failcnt']
				self.mem_swap_limit = container.attrs['HostConfig']['MemorySwap']
			#	self.mem_swap_faults = int(container.get_cgroup_item('memory.memsw.failcnt'))
				self.mem_stats = stats['memory_stats']['stats']
				self.mem_swappiness = container.attrs['HostConfig']['MemorySwappiness']
			#	self.disk_stats = stats['blkio_stats']
				self.disk_recursive_stats = stats['blkio_stats']['io_service_bytes_recursive']
			#	self.disk_th_stats = {}
			#	self.disk_th_recursive_stats = {}
				# Host stats
				self.host_memory_stats = psutil.virtual_memory()
				self.host_swap_stats = psutil.swap_memory()

		except Exception as err:
			logging.error('Fail to update container %s stats', self.name)
			exc_type, exc_obj, exc_tb = sys.exc_info()
			logging.error('Error: %s Line: %s', exc_type, exc_tb.tb_lineno)
			logging.error('Error: %s', err)
			pass


	def updateStatus(self, status):
		if status == 'running':
			return 'RUNNING'
		elif status == 'paused':
			return 'PAUSED'
		elif status == 'created':
			return 'CREATED'
		elif status == 'exited':
			if self.state in ['QUEUED', 'SUSPENDED', 'FINISHED']:
				return self.state
			else:
				return 'FINISHED'


	def update2(self):
		client = docker.from_env()

		if self.checkContainer():
			container = client.containers.get(self.name)
			stats = container.stats(stream=False)
			logging.debug('Updating container %s info with state %s', container.name, self.state)

			for x in range(3):
				str_error = None
				logging.info('Container %s Update Try %d', container.name, (x+1))

				try:
					if self.state in ['RUNNING','PAUSED']:
					#	self.ip = container.get_ips()
						self.cpu_set = container.attrs['HostConfig']['CpusetCpus']
						self.cpu_used = stats['cpu_stats']['cpu_usage']
						self.mem_limit = container.attrs['HostConfig']['Memory']
						#self.mem_faults = stats['memory_stats']['failcnt']
						self.mem_swap_limit = container.attrs['HostConfig']['MemorySwap']
					#	self.mem_swap_faults = int(container.get_cgroup_item('memory.memsw.failcnt'))
						self.mem_stats = stats['memory_stats']['stats']
						self.mem_swappiness = container.attrs['HostConfig']['MemorySwappiness']
					#	self.disk_stats = stats['blkio_stats']
					#	self.disk_recursive_stats = stats['blkio_stats']['io_service_bytes_recursive']
					#	self.disk_th_stats = {}
					#	self.disk_th_recursive_stats = {}

				except Exception as err:
					logging.error('Fail to update container %s stats - Try %d', self.name, (x+1))
					str_error = err
					logging.error('Error: %s', str_error)
					pass

				if not str_error:
					break


	# Método para verificar se um container existe


	def checkContainer(self):
		client = docker.from_env()
		check = False

		try:
			client.containers.get(self.name)
			check = True

		except docker.errors.NotFound as err:
			check = False

		if check:
			print('Container exist!')
		else:
			print('Container no exist!')

		return check


	# Function to create and start a docker container

	def startContainer2(self):
		client = docker.from_env()
		logging.info('Starting Container %s', self.name)

		try:
			client.containers.run(image=self.template, command=self.command, name=self.name, detach=True, security_opt=['seccomp:unconfined'])
			self.start_time = datetime.now()
			logging.info('Container %s Started with Success', self.name)

		except Exception as err:
			logging.error('Fail to Start the Container %s with Error: %s', self.name, err)


	def startContainer(self, memory_limit:int, swap_limit:int, cpuset:str):
		client = docker.from_env()
		logging.info('Starting Container %s', self.name)

		try:
			#client.containers.run(image=self.template, command=self.command, name=self.name, detach=True, mem_limit=memory_limit, memswap_limit=swap_limit, mem_swappiness=60, cpuset_cpus=cpuset)
			client.containers.run(image=self.template, command=self.command, name=self.name, detach=True, mem_limit=memory_limit, memswap_limit=swap_limit, cpuset_cpus=cpuset, security_opt=['seccomp:unconfined'])
			self.start_time = datetime.now()
			logging.info('Container %s Started with Success', self.name)

		except Exception as err:
			logging.error('Fail to Start the Container %s with Error: %s', self.name, err)


	# Function to stop a docker container


	def stopContainer(self):
		client = docker.from_env()
		logging.info('Stopping Container %s', self.name)

		try:
			container = client.containers.get(self.name)
			container.stop()
			logging.info('Container %s Stopped with Success', self.name)

		except Exception as err:
			logging.error('Fail to Stop the Container %s with Error: %s', self.name, err)


	# Function to pause a docker container


	def pauseContainer(self):
		client = docker.from_env()
		logging.info('Pausing Container %s', self.name)

		try:
			container = client.containers.get(self.name)
			container.pause()
			logging.info('Container %s Paused with Success', self.name)

		except Exception as err:
			logging.error('Fail to Pause the Container %s with Error: %s', self.name, err)


	# Function to unpause a docker container


	def unpauseContainer(self):
		client = docker.from_env()
		logging.info('Unpausing Container %s', self.name)

		try:
			container = client.containers.get(self.name)
			container.unpause()
			logging.info('Container %s Unpaused with Success', self.name)

		except Exception as err:
			logging.error('Fail to Unpause the Container %s with Error: %s', self.name, err)


	# Function to suspend a docker container


	def suspendContainer(self):
		client = docker.from_env()
		logging.info('Suspending Container %s', self.name)

		try:
			container = client.containers.get(self.name)

			if container.status == 'running':
				subprocess.run(['docker', 'checkpoint', 'create', self.name, 'checkpoint0'])
				self.state = 'SUSPENDED'

			else:
				logging.error('Container %s is not Running', self.name)

		except Exception as err:
			logging.error('Fail to Suspended the Container %s with Error: %s', self.name, err)


	# Function to resume a docker container


	def resumeContainer(self):
		client = docker.from_env()
		logging.info('Resuming Container %s', self.name)

		try:
			container = client.containers.get(self.name)
			if (container.status == 'exited') and (self.state == 'SUSPENDED'):
				subprocess.run(['docker', 'start', '--checkpoint', 'checkpoint0', self.name])
				subprocess.run(['docker', 'checkpoint', 'rm', self.name, 'checkpoint0'])

		except Exception as err:
			logging.error('Fail to Resume the Container %s with Error: %s', self.name, err)


	# Function to destroy a docker container


	def destroyContainer(self):
		client = docker.from_env()
		logging.info('Destroying Container %s', self.name)

		try:
			container = client.containers.get(self.name)
			container.remove()
			logging.info('Container %s Destroyed with Success', self.name)

		except Exception as err:
			logging.error('Fail to Destroy Container %s with Error: %s', self.name, err)


	# Function to pining cores for a docker container, using cgroups


	def setCPUCores(self, cores=''):
		client = docker.from_env()
		logging.info('Container %s Old Core Set: %s', self.name, self.cpu_set)
		logging.info('Container %s New Core Set: %s', self.name, cores)

		try:
			container = client.containers.get(self.name)

			if container.status == 'running':
				container.update(cpuset_cpus=cores)

		except Exception as err:
			logging.error('Fail in Setup Core Affinity for the Container %s with Error: %s', self.name, err)


	# Function to setup memory and swap limits for a docker container, using cgroups


	def setMemLimit(self, limit='', swap=''):
		client = docker.from_env()
		logging.info('Container %s old limit: %s', self.name, self.mem_limit)
		logging.info('Container %s set new memory limit: %s', self.name, limit)

		try:
			container = client.containers.get(self.name)

			if container.status == 'running':
				container.update(mem_limit=limit, memswap_limit=swap)

		except Exception as err:
			logging.error('Fail in Setup Memory Limit for the Container %s with Error: %s', self.name, err)
