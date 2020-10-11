import logging
from datetime import datetime
from classes.host import Host
from configparser import ConfigParser

parser = ConfigParser()
parser.read('./config/local-config.txt')

if parser['Container']['type'] == 'DOCKER':
	from classes.container import ContainerDocker as Container

elif parser['Container']['type'] == 'LXC':
	from classes.container import ContainerLXC as Container

log_basic = logging.getLogger('Container_Manager.Basic')

class Basic:

	def __init__(self):
		self.NAHM = 0
		self.level = ''

	def getNAHM(self):
		return self.NAHM

	def setNAHM(self, nahm):
		self.NAHM = nahm

	def getLevel(self):
		return self.level

	def setLevel(self, level):
		self.level = level


	def qos_share_limit_policy(self, host: Host):

		total_containers = host.active_list_counter() + host.inactive_list_counter()
		total_max_memory = host.get_max_usable_memoryPG()

		if self.level == 'BEST':

			if(host.active_list_counter() > 0):
				total_used = host.get_container_total_usedPG()
				local_NAHM = total_max_memory - total_used
				log_basic.info('Effective Not Used NAHM: %d', local_NAHM)
				shared_local_NAHM = round(local_NAHM / total_containers)

				for container in host.container_active_list:

					if (container.getContainerState() == 'RUNNING') and (shared_local_NAHM > 0):
						mem_used = container.getUsedMemoryPG()
						mem_limit = container.getMemoryLimitPG()
						log_basic.info('C: %s, CMU: %d, CML: %d', container.name, mem_used, mem_limit)

						new_limit = mem_used + shared_local_NAHM
						local_NAHM -= shared_local_NAHM
						container.setMemLimit2(new_limit)
						log_basic.info('Best Effort Adjusts Container: %s, new CML: %d', container.name, container.getMemoryLimitPG())

				self.NAHM = local_NAHM
				log_basic.info('Remain NAHM to start new containers: %d', self.NAHM)

		elif self.level == 'FAIR':

			new_limit = round(total_max_memory / total_containers)

			if host.active_list_counter() > 0:

				for container in host.container_active_list:

					if container.getContainerState() == 'RUNNING':
						mem_limit = container.getMemoryLimitPG()
						log_basic.info('C: %s, CML: %d', container.name, mem_limit)

						if new_limit < container.getMinMemoryLimitPG():
							new_limit = container.getMinMemoryLimitPG()

						delta = mem_limit - new_limit
						self.NAHM += delta
						container.setMemLimit2(new_limit)
						log_basic.info('Fair Share Stolen Container: %s, Delta: %d, new CML T1\u25BC: %d, new NAHM\u25B2: %d', container.name,
										delta, container.getMemoryLimitPG(), self.NAHM)


	def qos_start_policy(self, host: Host):
		sorted_list = sorted(host.container_inactive_list, key=lambda container: container.getInactiveTime(), reverse=True)
		index = 0
		log_basic.info('Available NAHM: %d', self.NAHM)

		if self.level == 'GUARANTEED':

			while(self.NAHM > 0) and (index < len(sorted_list)):
				container = sorted_list[index]

				if (container.getContainerState() == 'QUEUED'):

					if (container.getMaxMemoryLimitPG() <= self.NAHM) and (host.has_free_cores() >= container.request_cpus):
						cpu_allocation = host.get_available_cores(container.request_cpus)

						if parser['Container']['type'] == 'LXC':
							container.startContainer()
							container.setMemLimit2(container.getMaxMemoryLimitPG())
							container.setCPUCores(cpu_allocation)

						host.container_active_list.append(container)
						host.container_inactive_list.remove(container)

						log_basic.info('Container %s moved during Start from Inactive -> Active with status %s.', container.name, container.state)

						container.inactive_time = 0
						self.NAHM -= container.getMemoryLimitPG()
						log_basic.info('C: %s, CML: %d, new NAHM\u2193: %d', container.name, container.getMemoryLimitPG(), self.NAHM)

				index += 1

		elif self.level == 'BEST':

			limit_division = round(self.NAHM / host.inactive_list_counter())

			while(self.NAHM > 0) and (index < len(sorted_list)):
				container = sorted_list[index]

				if (container.getContainerState() == 'QUEUED'):

					if (container.getMinMemoryLimitPG() <= limit_division) and (host.has_free_cores() >= container.request_cpus):
						cpu_allocation = host.get_available_cores(container.request_cpus)

						if parser['Container']['type'] == 'LXC':
							container.startContainer()
							container.setMemLimit2(limit_division)
							container.setCPUCores(cpu_allocation)

						host.container_active_list.append(container)
						host.container_inactive_list.remove(container)

						log_basic.info('Container %s moved during Start from Inactive -> Active with status %s.', container.name, container.state)

						container.inactive_time = 0
						self.NAHM -= container.getMemoryLimitPG()
						log_basic.info('C: %s, CML: %d, new NAHM\u2193: %d', container.name, container.getMemoryLimitPG(), self.NAHM)

				index += 1

		elif self.level == 'FAIR':

			limit_division = round(self.NAHM / host.inactive_list_counter())

			while(self.NAHM > 0) and (index < len(sorted_list)):
				container = sorted_list[index]

				if (container.getContainerState() == 'QUEUED'):

					if (container.getMinMemoryLimitPG() <= limit_division) and (host.has_free_cores() >= container.request_cpus):
						cpu_allocation = host.get_available_cores(container.request_cpus)

						if container.getMaxMemoryLimitPG() > limit_division:
							new_limit = limit_division

						else:
							new_limit = container.getMaxMemoryLimitPG()

						if parser['Container']['type'] == 'LXC':
							container.startContainer()
							container.setMemLimit2(new_limit)
							container.setCPUCores(cpu_allocation)

						host.container_active_list.append(container)
						host.container_inactive_list.remove(container)

						log_basic.info('Container %s moved during Start from Inactive -> Active with status %s.', container.name, container.state)

						container.inactive_time = 0
						self.NAHM -= container.getMemoryLimitPG()
						log_basic.info('C: %s, CML: %d, new NAHM\u2193: %d', container.name, container.getMemoryLimitPG(), self.NAHM)

				index += 1


	def qos_recovery_limit_policy(self, host: Host):
		limit_division = round(self.NAHM / host.active_list_counter())

		for container in host.container_active_list:
			mem_limit = container.getMemoryLimitPG()
			max_limit = container.getMaxMemoryLimitPG()
			log_basic.info('C: %s, CML: %d', container.name, mem_limit)

			if self.level == 'FAIR':

				if ((mem_limit + limit_division) > max_limit) and (mem_limit != max_limit):
					self.NAHM -= max_limit - mem_limit
					container.setMemLimit2(max_limit)
					log_basic.info('Readjusting to Max Container: %s, new CML T1\u25B2: %d, new NAHM: %d\u25BC', container.name,
									container.getMemoryLimitPG(), self.NAHM)

				elif (mem_limit + limit_division) < max_limit:
					new_limit = mem_limit + limit_division
					self.NAHM -= limit_division
					container.setMemLimit2(new_limit)
					log_basic.info('Readjusting Container: %s, new CML T1\u25B2: %d, new NAHM\u25BC: %d', container.name,
									container.getMemoryLimitPG(), self.NAHM)

			elif self.level == 'BEST':
				new_limit = mem_limit + limit_division
				self.NAHM -= limit_division
				container.setMemLimit2(new_limit)
				log_basic.info('Readjusting Container: %s, new CML T1\u25B2: %d, new NAHM\u25BC: %d', container.name,
								container.getMemoryLimitPG(), self.NAHM)
