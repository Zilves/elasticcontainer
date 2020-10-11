import logging
import mmap
import psutil
import database
import functions
from datetime import datetime
from classes.host import Host
from configparser import ConfigParser
from threading import Thread
import multiprocessing as mp


parser = ConfigParser()
parser.read('./config/local-config.txt')

if parser['Container']['type'] == 'DOCKER':
	from classes.container import ContainerDocker as Container

elif parser['Container']['type'] == 'LXC':
	from classes.container import ContainerLXC as Container

log_plc = logging.getLogger('Container_Manager.Policies')


# ---------- Global Scheduling Policies ----------


def global_scheduler_policy(host_list, req_list, new_list):
	used_hosts = []

	if host_list:
		for request in new_list:
			app = database.get_application_from_ID(request.application)
			found = functions.request_bin_packing(host_list, used_hosts, request, app)

			if found:
				if request.status == 'QUEUED':
					request.status = 'SCHEDULED'
					database.update_request_status(request.reqid, request.status)
					req_list.append(request)
					new_list.remove(request)

		if not new_list:
			logging.debug('All Requests are Scheduled')

	else:
		logging.debug('No Hosts Available for Scheduling')


# ---------- Local Scaling Policies ----------

# Start Container Policy
# - Only starts the queued containers

def start_container_policy(host: Host, NAHM):
	sorted_list = sorted(host.container_inactive_list, key=lambda container: container.getInactiveTime(), reverse=True)
	index = 0

	while(NAHM > 0) and (index < len(sorted_list)):
		container = sorted_list[index]

		if (container.getContainerState() == 'QUEUED'):

			if (container.getMinMemoryLimitPG() <= NAHM) and (host.has_free_cores() >= container.request_cpus):
				cpu_allocation = host.get_available_cores(container.request_cpus)

				if parser['Container']['type'] == 'LXC':
					container.startContainer()
					container.setMemLimit2(container.getMinMemoryLimitPG())
					container.setCPUCores(cpu_allocation)

				elif parser['Container']['type'] == 'DOCKER':
					swap = container.getMaxMemoryLimit() + psutil.swap_memory().total
					container.startContainer(memory_limit=container.request_mem, swap_limit=swap, cpuset=cpu_allocation)

				host.container_active_list.append(container)
				host.container_inactive_list.remove(container)

				logging.info('Container %s moved during Start from Inactive -> Active with status %s.', container.name, container.state)

				container.inactive_time = 0
				NAHM -= container.getMemoryLimitPG()
				logging.info('new NAHM\u2193: %d', NAHM)

		index += 1


def start_all_containers(host: Host):
	sorted_list = sorted(host.container_inactive_list, key=lambda container: container.getInactiveTime(), reverse=True)
	index = 0

	while (index < len(sorted_list)):
		container = sorted_list[index]
		print('Container:', container.name, 'State:', container.state)

		if (container.getContainerState() == 'QUEUED'):

			if (host.has_free_cores() >= container.request_cpus):
				cpu_allocation = host.get_available_cores(container.request_cpus)

				if parser['Container']['type'] == 'LXC':
					container.startContainer()
					container.setMemLimit2(container.getMinMemoryLimitPG())
					container.setCPUCores(cpu_allocation)

				elif parser['Container']['type'] == 'DOCKER':
					swap = container.getMinMemoryLimit() + psutil.swap_memory().total
					container.startContainer(memory_limit=container.request_mem, swap_limit=swap, cpuset=cpu_allocation)

				host.container_active_list.append(container)
				host.container_inactive_list.remove(container)
				container.inactive_time = 0
				log_plc.info('Container %s moved during Start from Inactive -> Active with status %s.', container.name, container.state)

		index += 1


# Suspended Policy


def suspend_pressure_policy(host: Host):
	for container in host.container_active_list:
		if(container.state == 'RUNNING') and (not container.mem_steal_check):
			if container.getUsedMemory() >= container.getMaxMemoryLimit():
				container.setContainerState('SUSPENDING')
				core_list = container.cpu_set.split()

				for core in core_list:
					host.core_allocation[int(core)] = False

				container.inactive_time = datetime.now()
				host.container_inactive_list.append(container)
				host.container_active_list.remove(container)
				print('Suspending container:', container.name)
				container.mem_steal_check = True
				#Thread(target = container.suspendContainer, daemon=True).start()
				ctx = mp.get_context('spawn')
				proc = ctx.Process(target=container.suspendContainer)
				proc.start()
				log_plc.info('Container %s moved during Suspension from Active -> Inactive with status %s.', container.name, container.state)


def resume_policy(host: Host):
	for container in host.container_inactive_list:
		if (container.state == 'SUSPENDED'):
			if (container.getMemoryState() == 'STEAL') and (container.getMemoryStateTime() > 10):
				if (host.has_free_cores() >= container.request_cpus):
					cpu_allocation = host.get_available_cores(container.request_cpus)
					container.setContainerState('RESUMING')
					host.container_active_list.append(container)
					host.container_inactive_list.remove(container)
					container.inactive_time = 0
					print('Resuming container:', container.name)
					#Thread(target=container.resumeContainer, args=(cpu_allocation,), daemon=True).start()
					ctx = mp.get_context('spawn')
					proc = ctx.Process(target=container.resumeContainer, args=(cpu_allocation,))
					proc.start()
					log_plc.info('Container %s moved during Resume from Inactive -> Active with status %s.', container.name, container.state)


# MEC Like Memory Scaling Policy


def memory_shaping_policy(host: Host):
	need_list = []
	urgent_list = []
	stable_list = []
	mem_need = 0
	mem_urgent_need = 0

	# Classification:
	# Calculate memory consumption based in the historical info window
	# Categorize the memory comportament and organize in lists

	print('Classification Phase', flush=True)

	for container in host.container_active_list:
		if container.state == 'RUNNING':
			consumption = database.get_container_memory_consumption2(container.name, 10)
			container.setMemoryState(consumption)
			mem_limit = container.getMemoryLimit()
			mem_used = container.getUsedMemory()
			print('Container: ', container.name, ' Using: ', mem_used, ' Limit: ', mem_limit, ' Mem_State: ', container.mem_state,
				  ' MU: ', consumption['memory'], ' SU: ', consumption['swap'], 'MJF: ', consumption['major_faults'])

			if container.getMemoryState() == 'RISING':
				delta = consumption['memory'] + consumption['swap']

				#if (container.getUsedMemory() + delta) >= container.getMemoryLimit():
				if (mem_used + delta) >= mem_limit:
					need_list.append({'container': container, 'delta': delta})
					logging.info('Need Container: %s, Using: %d, Delta: %d, Limit: %d',
								container.name, mem_used, delta, mem_limit)
					mem_need += delta

				if consumption['major_faults'] > 0:
					#delta = (consumption['page_faults'] + consumption['major_faults']) * mmap.PAGESIZE
					delta = consumption['major_faults'] * mmap.PAGESIZE
					urgent_list.append({'container': container, 'delta': delta})
					logging.info('Urgent Container: %s, Using: %d, Delta: %d, Limit: %d',
								container.name, mem_used, delta, mem_limit)
					mem_urgent_need += delta

			else:
				if container.getMemoryStateTime() > 10:
					stable_list.append(container)
					logging.info('Stable Container: %s, Using: %d, Limit: %d', container.name, mem_used, mem_limit)

	# First Recover:
	# Recover some memory from FALLING and STABLE containers with Threshold less than 90%

	available_limit = host.get_available_limit()
	logging.info('Available Limit to be distribute: %d', available_limit)

	print('Light Recovery Phase', flush=True)
	print('Available: ', available_limit, ' Need: ', mem_need, ' Urgent: ', mem_urgent_need, flush=True)

	for container in stable_list:
		if container.getMemoryThreshold() < 90:
			delta = container.getMemoryLimit() // 10
			container.setMemLimit(limit=str(container.mem_limit - delta), swap=str(container.mem_swap_limit - delta))
			available_limit += delta
			print('Available: ', available_limit, flush=True)

	# Distribute Memory
	# Distribute memory over the containers if the request is lower than the available memory limit

	print('Distribution Phase', flush=True)
	print('Available: ', available_limit, ' Need: ', mem_need, ' Urgent: ', mem_urgent_need, flush=True)

	if (mem_need > 0) and (mem_need <= available_limit):
		for item in need_list:
			container = item['container']
			delta = item['delta']
			old_limit = container.getMemoryLimit()
			old_swap_limit = container.getSwapLimit()
			container.setMemLimit(limit=str(old_limit + delta), swap=str(old_swap_limit + delta))
			print('Container ', container.name, ' updated limit to ', old_limit + delta, flush = True)
			available_limit -= delta
			print('Available: ', available_limit, flush=True)

	elif (mem_urgent_need > 0) and (mem_urgent_need <= available_limit):
		for item in urgent_list:
			container = item['container']
			delta = item['delta']
			old_limit = container.getMemoryLimit()
			old_swap_limit = container.getSwapLimit()
			container.setMemLimit(limit=str(container.mem_limit + delta), swap = str(container.mem_swap_limit + delta))
			print('Container ', container.name, ' updated limit to ', old_limit + delta, flush = True)
			available_limit -= delta
			print('Available: ', available_limit, flush=True)

	elif (mem_urgent_need > 0):
		print('Critical State 1: Insufficient Memory for all Urgent Containers')
		urgent_list.sort(key=lambda item: item['container'].getRunningTime(), reverse=True)
		index = 0

		while (available_limit > 0) and (index < len(urgent_list)):
			container = urgent_list[index]['container']
			needed = urgent_list[index]['delta']

			print('Container: ', container.name, ' Needed: ', needed)

			if (available_limit - needed) > 0:
				old_limit = container.getMemoryLimit()
				old_swap_limit = container.getSwapLimit()
				container.setMemLimit(limit=str(container.mem_limit + needed), swap = str(container.mem_swap_limit + needed))
				print('Container ', container.name, ' updated limit to ', old_limit + delta, flush = True)
				available_limit -= needed
				print('Available: ', available_limit, flush=True)

			index += 1

	# Activate recover memory policy from stable
	# Force to use swap for good

	print('Heavy Recovery Phase', flush=True)
	print('Available: ', available_limit, ' Need: ', mem_need, ' Urgent: ', mem_urgent_need, flush=True)

	steal_check = False

	if (available_limit <= mem_need):
		if stable_list:
			for container in stable_list:
				delta = int(container.mem_stats['inactive_anon'])

				if delta > 0:
					container.setMemLimit(limit=str(container.mem_limit - delta), swap=str(container.mem_swap_limit - delta))
					available_limit += delta
					print('Available: ', available_limit, flush=True)
					steal_check = True

	if (available_limit <= mem_need):
		print('Critical State 2: Suspend a Container')
		sorted_list = sorted(host.container_active_list, key=lambda container: container.getRunningTime())
		index = 0

		while (available_limit <= mem_need) and (index < len(sorted_list)):
			container = sorted_list[index]

			if container not in stable_list:
				available_limit += container.getMemoryLimit()

				#Parallel Suspension Thread Creation and Execution
				container.state = 'SUSPENDING'
				core_list = container.cpu_set.split()

				for core in core_list:
					host.core_allocation[int(core)] = False

				container.inactive_time = datetime.now()
				host.container_inactive_list.append(container)
				host.container_active_list.remove(container)
				logging.info('Container %s moved during Suspension from Active -> Inactive with status %s.', container.name, container.state)
				print('Container: ', container.name, ' State: ', container.state)
				print('Available: ', available_limit, flush=True)
				Thread(target = container.suspendContainer).start()

				steal_check = True

			index += 1

	# Start new containers or restart suspended containers

	if steal_check == False:
		print('Start/Resume Phase', flush=True)
		print('Available: ', available_limit, ' Need: ', mem_need, ' Urgent: ', mem_urgent_need, flush=True)

		#sorted_list = sorted(host.container_inactive_list, key=lambda container: container.start_time, reverse=True)
		sorted_list = sorted(host.container_inactive_list, key=lambda container: container.getInactiveTime(), reverse=True)
		print('Lista Ordenada:', sorted_list)

		index = 0

		while (available_limit > 0) and (index < len(sorted_list)):
			container = sorted_list[index]

			if (container.state == 'SUSPENDED'):
				if (container.getMemoryLimit() <= available_limit) and (host.has_free_cores() >= container.request_cpus):
					print('Restart container ', container.name)
					cpu_allocation = host.get_available_cores(container.request_cpus)
					container.state = 'RESUMING'
					Thread(target = container.resumeContainer, args=(cpu_allocation,)).start()
					host.container_active_list.append(container)
					host.container_inactive_list.remove(container)
					logging.info('Container %s moved during Resume from Inactive -> Active with status %s.', container.name, container.state)
					container.inactive_time = 0
					available_limit -= container.mem_limit
					print('Available: ', available_limit, flush=True)

			elif (container.state in ['CREATED', 'NEW']):
				if (container.request_mem <= available_limit) and (host.has_free_cores() >= container.request_cpus):
					cpu_allocation = host.get_available_cores(container.request_cpus)
					swap = container.request_mem + psutil.swap_memory().total

					if(cpu_allocation != ''):
						if parser['Container']['type'] == 'LXC':
							container.startContainer()
							container.setMemLimit(str(container.request_mem), str(swap))
							container.setCPUCores(cpu_allocation)

						elif parser['Container']['type'] == 'DOCKER':
							container.startContainer(memory_limit=container.request_mem, swap_limit=swap, cpuset=cpu_allocation)

						host.container_active_list.append(container)
						host.container_inactive_list.remove(container)
						logging.info('Container %s moved during Start from Inactive -> Active with status %s.', container.name, container.state)
						container.inactive_time = 0
						available_limit -= container.request_mem
						print('Available: ', available_limit, flush=True)

			index += 1


# Elastic Docker Like Memory Scaling Policy


def ED_policy(host, free_mem, cooldown_list):

	for container in host.container_active_list:

		if (container.state == 'RUNNING'):
			check = False
			cooldown = next((item for item in cooldown_list if item['name'] == container.name), None)

			if(not cooldown):
				check = True

			elif((cooldown['breath'] == 'UP') and ((datetime.now() - cooldown['last_time']).seconds > 10)):
				check = True

			elif((cooldown['breath'] == 'DOWN') and ((datetime.now() - cooldown['last_time']).seconds > 20)):
				check = True

			if check:
				media = database.get_container_memory_consumption_ED(container.name, 10)
				threshold = (media * 100) // container.mem_limit
				print('Threshold: ', threshold)
				breath = ''

				if (threshold > 90):
					delta = 256 * (2 ** 20)
					logging.info('Container %s Delta: %d', container.name, delta)
					container.setMemLimit(limit=str(container.mem_limit + delta), swap=str(container.mem_swap_limit + delta))
					breath = 'UP'
					free_mem -= delta

				elif (threshold < 70):
					delta = 128 * (2 ** 20)
					logging.info('Container %s Delta: %d', container.name, delta)
					container.setMemLimit(limit=str(container.mem_limit - delta), swap=str(container.mem_swap_limit - delta))
					breath = 'DOWN'
					free_mem -= delta

				print('Breath:' + breath)

				if (breath != ''):
					if cooldown:
						cooldown['breath'] = breath
						cooldown['last_time'] = datetime.now()

					else:
						cooldown = {'name':container.name, 'breath':breath, 'last_time':datetime.now()}
						cooldown_list.append(cooldown)

				else:
					if cooldown:
						cooldown_list.remove(cooldown)

	return free_mem
