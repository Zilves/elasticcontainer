import logging
import mmap
import psutil
import database
import functions
from datetime import datetime
from classes.host import Host
from configparser import ConfigParser
from threading import Thread


parser = ConfigParser()
parser.read('./config/local-config.txt')

if parser['Container']['type'] == 'DOCKER':
	from classes.container import ContainerDocker as Container

elif parser['Container']['type'] == 'LXC':
	from classes.container import ContainerLXC as Container


# ---------- Global Scheduling Policies ----------


def global_scheduler_policy(host_list, req_list, new_list): # Em dev
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


def start_container_policy(host, free_mem): #Em dev
    sorted_list = sorted(host.container_inactive_list, key=lambda container: container.start_time, reverse=True)
    print('Lista Ordenada:', sorted_list)

    for container in sorted_list:
        if (container.state == 'SUSPENDED'):
            if(container.mem_limit < free_mem):
                print('Restart container %s', container.name)

        elif (container.state in ['CREATED', 'NEW']):
            cpu_allocation = host.get_available_cores(container.request_cpus)
            swap = container.request_mem + psutil.swap_memory().total

            if(cpu_allocation != '') and (container.request_mem <= free_mem):
                if parser['Container']['type'] == 'LXC':
                    container.startContainer()
                    container.setMemLimit(str(container.request_mem), str(swap))
                    container.setCPUCores(cpu_allocation)

                elif parser['Container']['type'] == 'DOCKER':
                    container.startContainer(memory_limit=container.request_mem, swap_limit=swap, cpuset=cpu_allocation)

                host.container_active_list.append(container)
                host.container_inactive_list.remove(container)
                free_mem -= container.request_mem


# MEC Like Memory Scaling Policy V1


def memory_shaping_policy(host, free_mem):
    for container in host.container_active_list:
        if (container.state == 'RUNNING'):
            delta, delta_swap = database.get_container_memory_consumption(container.name, 10)
            threshold = ((container.getUsedMemory() + delta) * 100) // container.mem_limit
            print('Threshold: ', threshold)

            if (delta > 0):
                if(threshold > 100) and (delta < free_mem):
                    logging.info('Container %s Delta: %d', container.name, delta)
                    container.setMemLimit(limit=str(container.mem_limit + delta), swap=str(container.mem_swap_limit + delta))
                    free_mem -= delta

            elif (delta < 0):
                logging.info('Container %s Delta: %d', container.name, delta)
                container.setMemLimit(limit=str(container.mem_limit + delta), swap=str(container.mem_swap_limit + delta))
                free_mem += delta

            else:
                print('Memory Consumable is Stable!')

    return free_mem


# MEC Like Memory Scaling Policy V2


def memory_shaping_policy_V2(host: Host):  # Em Dev
	need_list = []
	limited_list = []
	stable_list = []
	mem_need = 0
	mem_limited_need = 0
	available_limit = host.get_available_limit()

	# Calculate memory consumption based in the historical info window, categorize the memory comportament and organize in lists

	for container in host.container_active_list:
		if (container.state == 'RUNNING'):
			consumption = database.get_container_memory_consumption2(container.name, 10)

			if (consumption['memory'] > 0) or (consumption['swap'] > 0):
				if (container.mem_state != 'RISING'):
					container.mem_state = 'RISING'
					container.mem_state_time = datetime.now()

				if container.mem_state == 'RISING':
					delta = consumption['memory'] + consumption['swap']
					need_list.append({'container': container, 'delta': delta})
					mem_need += delta

					if(consumption['major_faults'] > 0):
						delta = (consumption['page_faults'] + consumption['major_faults']) * mmap.PAGESIZE
						limited_list.append({'container': container, 'delta': delta})
						mem_limited_need += delta

			elif (consumption['memory'] < 0):
				if (container.mem_state != 'FALLING'):
					container.mem_state = 'FALLING'
					container.mem_state_time = datetime.now()

				if (container.mem_state == 'FALLING') and (container.getMemoryStateTime > 10):
					if container.getMemoryThreshold() <= 50:
						delta = container.mem_limit // 4
						container.setMemLimit(limit=str(container.mem_limit - delta), swap=str(container.mem_swap_limit - delta))
						available_limit += delta

			else:
				if (consumption['swap'] <= 0) and (consumption['major_faults'] == 0):
					if (container.mem_state != 'STABLE'):
						container.mem_state = 'STABLE'
						container.mem_state_time = datetime.now()

					if (container.state == 'STABLE') and (container.getMemoryStateTime > 10):
						stable_list.append(container)

	# Distribute memory over the containers if the request is lower than the available memory limit

	if mem_need <= available_limit:
		for item in need_list:
			container = item['container']
			delta = item['delta']
			container.setMemLimit(limit=str(container.mem_limit + delta), swap=str(container.mem_swap_limit + delta))
			available_limit -= delta

	elif mem_limited_need <= available_limit:
		for item in limited_list:
			container = item['container']
			delta = item['delta']
			container.setMemLimit(limit=str(container.mem_limit + delta), swap = str(container.mem_swap_limit + delta))
			available_limit -= delta

	else:
		print('Critical State 1: Needs Some Recover')

	# Activate recover memory policy from stable

	if available_limit <= 0:
		if stable_list:
			recover = 0
			for container in stable_list:
				delta = int(container.mem_stats['inactive_anon'])

				if delta > 0:
					container.setMemLimit(limit=str(container.mem_limit - delta), swap=str(container.mem_swap_limit - delta))
					recover += delta

		else:
			print('Critical State 2: Suspend a Container')


# V3


def memory_shaping_policy_V3(host: Host):  # Em Dev
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
			print('Container: ', container.name, ' Mem_State: ', container.mem_state, ' MU: ', consumption['memory'],
			' SU: ', consumption['swap'], 'MJF: ', consumption['major_faults'])

			if container.getMemoryState() == 'RISING':
				delta = consumption['memory'] + consumption['swap']

				if (container.getUsedMemory() + delta) >= container.getMemoryLimit():
					need_list.append({'container': container, 'delta': delta})
					logging.info('Need Container: %s, Using: %d, Delta: %d, Limit: %d',
								container.name, container.getUsedMemory(), delta, container.getMemoryLimit())
					mem_need += delta

				if consumption['major_faults'] > 0:
					delta = (consumption['page_faults'] + consumption['major_faults']) * mmap.PAGESIZE
					urgent_list.append({'container': container, 'delta': delta})
					logging.info('Urgent Container: %s, Using: %d, Delta: %d, Limit: %d',
								container.name, container.getUsedMemory(), delta, container.getMemoryLimit())
					mem_urgent_need += delta

			else:
				if container.getMemoryStateTime() > 10:
					stable_list.append(container)
					logging.info('Stable Container: %s, Using: %d, Limit: %d',
								container.name, container.getUsedMemory(), container.getMemoryLimit())


	# First Recover:
	# Recover some memory from FALLING and STABLE containers with Threshold less than 70%

	available_limit = host.get_available_limit()

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
	#else:
	#	print('Migration policy here')


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
