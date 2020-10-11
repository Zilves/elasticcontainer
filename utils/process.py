import time
import logging
import communication
import database
import scheduler
import policies
import socket
import nosqlbase
import mmap
from vemoc.vemoc import VEMOC
from vemoc.basic import Basic
from classes.host import Host
from datetime import datetime
from configparser import ConfigParser
import multiprocessing as mp


# Host Monitor function/process:
# - Collects host and container information, every second (with sleep)
# - Publish container information as a historic in a local database
# - Send a host complete object (with containers) to the Global Monitor
# - Remove finished containers from host.
# OBS: Need improvement in send_monitor_data function (some connections are refused by global)


def host_monitor(shared_list: list):
	logging.basicConfig(filename='./log/host-monitor.log', filemode='a', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

	host = Host()

	while True:
		logging.info('========================================================')
		logging.debug('Starting Host Monitor')
		start_time = datetime.now()

		try:
			update_lat_init = datetime.now()
			host.update()
			host.container_active_list = shared_list[0]
			host.container_inactive_list = shared_list[1]
			#host.core_allocation = shared_list[2]
			#host.update_containers()
			host.update_containers2()
			update_lat_end = datetime.now()
			logging.info('Get Host + Containers Info Latency: %f', (update_lat_end - update_lat_init).total_seconds())

			#print('Monitor:\n', 'AC:', host.container_active_list, 'IC:', host.container_inactive_list, 'Core:', host.core_allocation)

			publish_lat_init = datetime.now()
			container_list = host.container_active_list + host.container_inactive_list
			logging.info('Container List:' + str(container_list))

			for container in container_list:
				if container.checkContainer():
					logging.debug('Publish Container %s Info', container.name)
					#database.publish_local_container_history(container)
					nosqlbase.publish_container_history(container)

			publish_lat_end = datetime.now()
			logging.info('Local publish Container Info Latency: %f', (publish_lat_end - publish_lat_init).total_seconds())

			send_lat_init = datetime.now()
			logging.debug('Send Monitoring Data to Manager')
			logging.debug('Sended Host Data: %s', vars(host))
			communication.send_monitor_data(host)
			send_lat_end = datetime.now()
			logging.info('Send Host Info to CM Latency: %f', (send_lat_end - send_lat_init).total_seconds())

			host.remove_finished_containers()
			#shared_list[2] = host.core_allocation

		except Exception as err:
			logging.error('Monitor error: %s', err)

		stop_time = datetime.now()
		monitor_time = (stop_time - start_time).total_seconds()
		logging.info('Monitor Total Time: %f, Next Sleep Time: %f', monitor_time, (1 - monitor_time))
		logging.info('========================================================')

		if monitor_time < 1:
			logging.debug('Host Monitor Sleeping')
			time.sleep(1 - monitor_time)


# Global Monitor function/process:
# - Receive host and their containers information, periodically
# - Update host and container status, keeping a container execution history in a database
# - Maintain a available host list with their most recent information
# - Updates user's requests until conclusion
# - OBS: Need improvement in receive_monitor_data function (refusing some host connections)


def global_monitor(host_list, request_list):
	logging.basicConfig(filename='./log/global-monitor.log', filemode='a', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

	counter = 1

	while True:
		data, host = communication.receive_monitor_data()

		if host:
			logging.debug('Received Data from Host %s', host.hostname)
			logging.debug('Received Host Data: %s', vars(host))
			logging.debug('Publish Host %s Info', host.hostname)
			database.publish_host(host.hostname, data)

			container_list = host.container_active_list + host.container_inactive_list

			for container in container_list:
				database.publish_container_history(container)
				database.update_container_status(container)

			if host in host_list:
				index = host_list.index(host)
				host_list[index] = host

			else:
				host_list.append(host)

			for request in request_list:
				index = request_list.index(request)
				request.check_container_status(container_list)
				modified = request.change_status()
				request_list[index] = request
				logging.debug('Monitoring Request: %s, Status: %s, Containers: %s', request_list[index].reqid,
							request_list[index].status, request_list[index].listcontainers)

				if modified:
					database.update_request_status(request.reqid, request.status)

				if request.status == 'FINISHED':
					logging.info('Request Finished: %s', request.reqid)
					request_list.remove(request)

			#print('Host List: ', host_list)
			print('Counter: ', counter)
			print('Request List: ', request_list)
			counter += 1


# Global Scheduler function/process:
# - Get the new user's requests from the database
# - Add it to a request queue
# - Calls a scheduler function to distribute the containers over the available hosts


def global_scheduler(host_list, request_list):
	logging.basicConfig(filename='./log/global-scheduler.log', filemode='a', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

	new_req_list = []

	while True:
		logging.debug('Starting Global Scheduler')
		new_req_list += database.get_new_requests()

		for req in new_req_list:
			if req.status == 'NEW':
				req.status = 'QUEUED'
				database.update_request_status(req.reqid, req.status)

		scheduler.one_host_global_scheduler(host_list, request_list, new_req_list)
		logging.debug('Global Scheduler Sleeping')
		time.sleep(5)


# Request Receiver function/process:
# - Treat each container received as a new requests
# - Separate the requests in threads


def request_receiver(entry_queue: mp.Queue):
	logging.basicConfig(filename='./log/request-receiver2.log', filemode='a', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

	config = ConfigParser()
	config.read('./config/local-config.txt')
	receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	receive_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)

	try:
		receive_socket.bind((socket.gethostname(), int(config['Manager']['local_receive_port'])))

	except socket.error as err:
		logging.error('Receiving Request from Global Error: %s', err)

	receive_socket.listen(5)

	while True:
		connection, address = receive_socket.accept()

		try:
			#Thread(target=communication.receive_thread, args=(connection, entry_queue)).start()
			ctx = mp.get_context('fork')
			proc_recv = ctx.Process(target=communication.receive_thread, args=(connection, entry_queue))
			proc_recv.start()

		except:
			logging.error('Thread not created')

	receive_socket.close()


# No manager

def no_manager(shared_list: list, entry_queue: mp.Queue):
	logNM = logging.getLogger('Container_Manager')
	logNM.setLevel(logging.INFO)
	format = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
	file_handler = logging.FileHandler(filename = './log/no-manager.log', mode = 'a')
	file_handler.setFormatter(format)
	file_handler.setLevel(logging.DEBUG)
	logNM.addHandler(file_handler)

	sched = Basic()

	config = ConfigParser()
	config.read('./config/local-config.txt')
	sched.setLevel(config['QoS']['level'])

	host = Host()
	sched_counter = 1

	while True:
		start_time = datetime.now()
		logNM.info('========================================================')
		logNM.info('Sched counter: %d', sched_counter)
		logNM.info('Sched init timestamp: %s', start_time)
		print(sched_counter, datetime.now())

		# Add Created Containers
		while not entry_queue.empty():
			container = entry_queue.get()
			logNM.info('New Container: %s', container.name)
			container.inactive_time = datetime.now()
			container.setContainerState('QUEUED')
			host.container_inactive_list.append(container)

		host.update()
		host.update_containers2()

		TCML, NAHM, HAM = host.get_host_memory_info()
		sched.setNAHM(NAHM)
		logNM.info('NAHM: %d, HAM: %d, TCML: %d', sched.getNAHM(), HAM, TCML)
		logNM.info('Active List: %s', host.container_active_list)
		logNM.info('Inactive List: %s', host.container_inactive_list)
		logNM.info('QoS Test: %s', sched.getLevel())

		if(host.inactive_list_counter() != 0):
			logNM.info('---------------------------------------------------------')
			logNM.info('Executing Limit Redistribution Policy:')
			sched.qos_share_limit_policy(host)
			logNM.info('---------------------------------------------------------')
			logNM.info('Executing Start Inactive Containers:')
			sched.qos_start_policy(host)

		else:
			if (sched.getNAHM() > 0) and (sched.getLevel() in ['BEST', 'FAIR']) and (host.active_list_counter() > 0):
				logNM.info('---------------------------------------------------------')
				logNM.info('Executing NAHM Redistribution:')
				sched.qos_recovery_limit_policy(host)

		host.update()
		host.update_containers2()

		shared_list[0] = host.container_active_list
		shared_list[1] = host.container_inactive_list

		stop_time = datetime.now()
		logNM.info('Sched end timestamp: %s', stop_time)
		latency = (stop_time - start_time).total_seconds()
		logNM.info('New Sched Latency: %f', latency)
		logNM.info('Sleep time: %f seconds', 1 - latency)
		logNM.info('========================================================')
		sched_counter += 1

		if (latency < 1):
			time.sleep(1 - latency)


# Container Manager default function/process:
# - Basic function to test simple algorithms or to monitor a simple test


def container_manager(shared_list: list, entry_queue: mp.Queue):
	logCM = logging.getLogger('Container_Manager')
	logCM.setLevel(logging.INFO)
	format = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
	file_handler = logging.FileHandler(filename = './log/container-manager.log', mode = 'a')
	file_handler.setFormatter(format)
	file_handler.setLevel(logging.INFO)
	logCM.addHandler(file_handler)

	host = Host()
	#cooldown_list = []

	while True:
		logCM.debug('Starting Container Manager')

		# Add Created Containers
		while not entry_queue.empty():
			container = entry_queue.get()
			logCM.info('New Container: %s', container.name)
			container.inactive_time = datetime.now()
			container.setContainerState('QUEUED')
			host.container_inactive_list.append(container)

		host.update()
		host.update_containers()

		#for cooldown in cooldown_list:
		#	if not host.is_active_container(cooldown['name']):
		#		cooldown_list.remove(cooldown)

		#free_mem = host.get_available_memory()
		free_mem = host.get_available_limit()
		logCM.info('Free Memory Before Policy: %d MiB', free_mem // 2 ** 20)
		#free_mem = policies.ED_policy(host, free_mem, cooldown_list)

		if (free_mem > 0) and host.has_free_cores() and host.has_inactive_containers():
			policies.start_container_policy(host, free_mem)

		logCM.info('Free Memory After Policy: %d MiB', free_mem // 2 ** 20)
		host.update()
		host.update_containers()
		shared_list[0] = host.container_active_list
		shared_list[1] = host.container_inactive_list
		shared_list[2] = host.core_allocation
		logCM.debug('Container Manager Sleeping')
		time.sleep(5)


# Container Manager for VEMOC:
# - Executes a serie of algorithms to manager container resources during their execution
# - Get the new entries from request receiver process to start the containers
# - Shares a picture of the host, with the host monitor, after each cycle


def container_manager2(shared_list: list, entry_queue: mp.Queue): # Algorithm 1
	logCM = logging.getLogger('Container_Manager')
	logCM.setLevel(logging.INFO)
	format = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
	file_handler = logging.FileHandler(filename = './log/container-manager2.log', mode = 'a')
	file_handler.setFormatter(format)
	file_handler.setLevel(logging.INFO)
	logCM.addHandler(file_handler)

	config = ConfigParser()
	config.read('./config/local-config.txt')

	host = Host()
	scheduler = VEMOC()
	scheduler.long_interval = int(config['Scheduler']['sched_interval'])
	scheduler.short_interval = max(int(scheduler.long_interval / 2), 3)
	scheduler.mem_write_rate = 9999
	scheduler.swapout_rate = 33000
	scheduler.swapin_rate = 7000
	#scheduler.MUE = float(config['Scheduler']['MUE'])
	HMUT = round((22.8 * 2 ** 30) / mmap.PAGESIZE)
	#maxMU = float(config['Scheduler']['MUE'])
	maxMU = 0.997
	scheduler.latency = 0.1
	sched_counter = 1

	while True:
		start_time = datetime.now()
		logCM.info('========================================================')
		logCM.info('Sched counter: %d', sched_counter)
		logCM.info('Sched init timestamp: %s', start_time)
		scheduler.reset()
		scheduler.sched_interval = scheduler.long_interval
		scheduler.sched_start_time = start_time

		# Add Created Containers

		while not entry_queue.empty():
			container = entry_queue.get()
			logCM.info('New Container: %s', container.name)
			container.inactive_time = datetime.now()
			container.setContainerState('QUEUED')
			host.container_inactive_list.append(container)

		# Count Inactive Container Memory

		inactive_memory = 0
		host.update()
		host.update_containers2()
		#host.remove_finished_containers()

		for container in host.container_inactive_list:

			if container.state == 'QUEUED':
				inactive_memory += container.getMinMemoryLimitPG()

			elif container.state == 'SUSPENDED':
				inactive_memory += container.getMemoryLimitPG() + container.getDeltaMemory()

		#scheduler.setNAHM(host.get_available_limit())
		#HAM = host.memory.available
		#TCML = host.get_container_total_limit()
		TCML, NAHM, HAM = host.get_host_memory_info()

		scheduler.setNAHM(NAHM)
		#scheduler.MUE = min(maxMU, maxMU - (HAM + TCML - HMUT) / (20 * 10 ** 9))
		if HAM  < 262144:
			scheduler.setMUE(min(maxMU, maxMU - (HAM + TCML - HMUT) / ((20 * 10 ** 9) / mmap.PAGESIZE)))

		else:
			scheduler.setMUE(maxMU)

		scheduler.spare_mem_cap = round(scheduler.mem_write_rate * (scheduler.sched_interval + scheduler.latency))

		logCM.info('NAHM: %d, HAM: %d, TCML: %d', scheduler.NAHM, HAM, TCML)
		logCM.info('MUE: %f, spare_mem_cap: %d, latency: %f', scheduler.MUE, scheduler.spare_mem_cap, scheduler.latency)
		logCM.info('Active List: ' + str(host.container_active_list))
		logCM.info('Inactive List: ' + str(host.container_inactive_list))

		# Call algorithm 2

		if len(host.container_active_list) > 0:
			logCM.info('---------------------------------------------------------')
			logCM.info('Executing Demand Estimation:')
			scheduler.mem_demand_estimation2(host)

		# Call algorithm 5

		if scheduler.getNAHM() < (scheduler.getMemoryNeeded() + scheduler.getMemoryUrgent() + scheduler.getPauseDemand() + inactive_memory):
			logCM.info('---------------------------------------------------------')
			logCM.info('Executing Passive Memory Reduction:' + str(scheduler.provider_list))
			scheduler.passive_memory_reduction2()

		# Call algorithm 6

		if scheduler.getNAHM() < (scheduler.getMemoryNeeded() + scheduler.getMemoryUrgent() + scheduler.getPauseDemand()):
			logCM.info('---------------------------------------------------------')
			logCM.info('Executing Active Memory Reduction:' + str(scheduler.provider_list))
			scheduler.active_memory_recovery3()

		# Call algorithm 7

		if (scheduler.getMemoryUrgent() > 0) or (scheduler.getMemoryNeeded() > 0) or (scheduler.getPauseDemand() > 0):
			logCM.info('---------------------------------------------------------')
			logCM.info('Executing Container Limits Adjusts:')
			scheduler.increase_container_memory_limits(host)

		#end_lat = datetime.now()
		#scheduler.latency = (end_lat - start_time).total_seconds()

		# Call algorithm 8

		if (scheduler.getMemoryUrgent() > 0) or (scheduler.getMemoryNeeded() > 0) or (scheduler.getPauseDemand() > 0):
			logCM.info('---------------------------------------------------------')
			logCM.info('Executing Pause/Suspend Running Containers:')
			scheduler.pause_suspend_running_containers(host)

		# Call algorithm 10

		elif (scheduler.getStealCheck() == False) and (len(host.container_inactive_list) != 0):
			logCM.info('---------------------------------------------------------')
			logCM.info('Executing Start/Resume Inactive Containers:')
			scheduler.start_resume_inactive_container(host)

		# Host Updates

		host.update()
		host.update_containers2()
		shared_list[0] = host.container_active_list
		shared_list[1] = host.container_inactive_list
		shared_list[2] = host.core_allocation

		# Calculate process sleep time

		stop_time = datetime.now()
		#sched_time = (stop_time - start_time).total_seconds()
		scheduler.latency = (stop_time - start_time).total_seconds()
		logCM.info('Sched end timestamp: %s', stop_time)
		logCM.info('New Sched Latency: %f', scheduler.latency)
		#logCM.info('Sched Time: %f seconds, Sleep time: %f seconds', sched_time, (scheduler.sched_interval - sched_time))
		logCM.info('Sleep time: %f seconds', scheduler.sched_interval - scheduler.latency - 0.007)
		logCM.info('========================================================')
		sched_counter += 1

		#if (sched_time < scheduler.sched_interval):
		#	time.sleep(scheduler.sched_interval - sched_time)

		if (scheduler.latency < scheduler.sched_interval):
			time.sleep(scheduler.sched_interval - scheduler.latency - 0.007)
