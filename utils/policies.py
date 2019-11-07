import logging
import mmap
import psutil
from . import database
from . import functions
from datetime import datetime
from classes.host import Host
from configparser import ConfigParser


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


# Recovery Memory Policy


def recovery_memory_policy(host): # Em Dev
	print('Em dev')


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
	rise_list = []
	stable_list = []
	rise_need = 0
	available_limit = host.get_available_limit()

	# Calculate memory consumption based in the historical info window, categorize the memory comportament and organize in lists

	for container in host.container_active_list:
		if (container.state == 'RUNNING'):
			consumption = database.get_container_memory_consumption2(container.name, 10)

			if (consumption['memory'] > 0) or (consumption['swap'] > 0):
				if (container.mem_state != 'RISING'):
					container.mem_state = 'RISING'
					container.mem_state_time = datetime.now()

				if(consumption['major_faults'] > 0):
					rise_list.append({'container': container, 'consumption': consumption})
					rise_need += consumption['major_faults'] * mmap.PAGESIZE

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

					stable_list.append(container)

	# Distribute memory over the containers if the request is lower than the available memory limit

	if rise_need < available_limit:
		for item in rise_list:
			container = item[container]
			delta = item['consumption']['major_faults'] * mmap.PAGESIZE
			container.setMemLimit(limit=str(container.mem_limit + delta), swap=str(container.mem_swap_limit + delta))
			available_limit -= delta

	else:
		print('have less mem than needed')

	# Activate recover memory policy from stable

	if available_limit <= 0:
		print('start recover policy')


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
