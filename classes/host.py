import psutil
import socket
import logging
from configparser import ConfigParser
from datetime import datetime


class Host:


### Host Class Constructor:
### Contains host resource usage information (CPU, Memory, Swap), cpu pinning list and containers information
### Has host reserve information for save some resources for host processes


	def __init__(self):
		config = ConfigParser()
		config.read('./config/local-config.txt')

		self.hostname = socket.gethostname()
		self.memory_reservation = int(config['Reserve']['memory'])
		self.core_reservation = int(config['Reserve']['cpu_cores'])
		# Host CPU stats
		self.cpu_count = psutil.cpu_count()
		self.cpu_load = psutil.cpu_percent(percpu=True)
		# Host Memory Stats
		self.memory = psutil.virtual_memory()
		self.swap = psutil.swap_memory()
		# Host Container Lists
		self.container_active_list = []
		self.container_inactive_list = []
		self.core_allocation = []

		for index in range(self.cpu_count):
			self.core_allocation.append(False)


### Equality Function to compare two instances and verify if they are equal


	def __eq__(self, other):
		if isinstance(other, self.__class__):

			if self.hostname == other.hostname:
				return True

			else:
				return False


### String Functions to indicate what is printed for an instance of this class


	def __str__(self):
		out = []

		for key in self.__dict__:
			out.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

		return ', '.join(out)


	def __repr__(self):
		return self.hostname


### Update Function for get most recent resource information from host


	def update(self):
		try:
			self.cpu_load = psutil.cpu_percent(percpu=True)
			self.memory = psutil.virtual_memory()
			self.swap = psutil.swap_memory()

		except Exception as err:
			logging.error('Host Update Error: %s', err)

		logging.debug('Monitoring Data Updated')
		logging.debug('Host Data: %s', vars(self))


### Function to calculate available memory from a host, reserving some memory for their processes
### Uses Linux Available memory as base information

	def get_available_memory(self):
		sum_limits = 0
		sum_mem_used = 0

		for container in self.container_active_list:
			if container.state == 'RUNNING':
				sum_limits += container.mem_limit
				sum_mem_used += container.getUsedMemory()

		container_reserve = abs(sum_limits - sum_mem_used)
		print('Container Total Reserve: ' + str(container_reserve // 2 ** 20) + 'MB')
		print('Container Total Limit: ' + str(sum_limits // 2 ** 20) + 'MB')
		print('Container Total Memory Used: ' + str(sum_mem_used // 2 ** 20) + 'MB')

		host_only_used = self.memory.used - sum_mem_used
		host_reserve = self.memory_reservation - host_only_used
		available = self.memory.available - (container_reserve + abs(host_reserve))
		print('Host Reserve: ' + str(host_reserve // 2 ** 20) + 'MB')
		print('Host Required Reservation: ' + str(self.memory_reservation // 2 ** 20) + 'MB')
		print('Host Only Memory Used: ' + str(host_only_used // 2 ** 20) + 'MB')
		print('Available Memory: ' + str(available // 2 ** 20) + 'MB')

		if available <= 0:
			return 0
		else:
			return available


### Function to calculate the total memory limit set for all running containers


	def get_container_total_limit(self):
		sum_limits = 0

		for container in self.container_active_list:
			if container.state == 'RUNNING':
				sum_limits += container.mem_limit

		return sum_limits


### Function to calculate the available limit based on the total limit set and the host total memory


	def get_available_limit(self):
		container_limit = self.get_container_total_limit()
		available = self.memory.total - container_limit - self.memory_reservation
		return available


### Function to verify if a host has cpu cores not set to any running containers


	def has_free_cores(self):
		free_cores = self.core_allocation.count(False) - self.core_reservation
		if free_cores > 0:
			return True

		else:
			return False


### Function to get a set of cores for cgroup cpuset of a container request


	def get_available_cores(self, request = int):
		free_cores = self.core_allocation.count(False) - self.core_reservation
		cpu_allocation = ''

		if free_cores >= request:
			#for core in range(request):
			#	index = self.core_allocation.index(False)
			#	cpu_allocation += (str)(index) + ','
			#	self.core_allocation[index] = True

			core = 0
			while core in range(request):
				index = self.core_allocation.index(False)
				cpu_allocation += (str)(index)
				self.core_allocation[index] = True
				core += 1

				if core < request:
					cpu_allocation += ','


		return cpu_allocation


### Function to verify if a host has inactive containers


	def has_inactive_containers(self):
		if self.container_inactive_list:
			return True

		else:
			return False


### Function to verify if a container is a active container in a host


	def is_active_container(self, name = str):
		check = False

		for container in self.container_active_list:
			if name == container.name:
				check = True

		return check


### Function to remove from host inactive list, containers that finishes their jobs
### Unset cpu cores allocated to removed containers and destroy the containers


	def remove_finished_containers(self):
		for container in self.container_inactive_list:
			if container.state == 'STOPPED':
				logging.info('Removing container %s with status %s', container.name, container.state)
				core_list = container.cpu_set.split()

				for core in core_list:
					logging.info('Finished Container Releasing Core %s', core)
					self.core_allocation[int(core)] = False

				self.container_inactive_list.remove(container)
				container.destroyContainer()


### Function to update the container resource information for each container allocated to the host


	def update_containers(self):
		for container in self.container_active_list:
			container.update()

		for container in self.container_inactive_list:
			container.update()

		for container in self.container_active_list:
			if container.state in ['STOPPED','SUSPENDED','SUSPENDING']:
				self.container_inactive_list.append(container)
				container.inactive_time = datetime.now()
				self.container_active_list.remove(container)
				logging.info('Container %s moved during Update from Active -> Inactive with status %s.', container.name, container.state)

		# Update Stats from Active List Containers
		for container in self.container_inactive_list:
			if container.state in ['RUNNING','RESUMING']:
				self.container_active_list.append(container)
				container.inactive_time = 0
				self.container_inactive_list.remove(container)
				logging.info('Container %s moved during Update from Inactive -> Active with status %s.', container.name, container.state)
