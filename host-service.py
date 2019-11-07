#!/usr/bin/env python3

import logging
from utils import process
from classes.host import Host
from multiprocessing import Process, Manager

if __name__ == '__main__':
	logging.basicConfig(filename='./log/host-service.log', filemode='w', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

	with Manager() as manager:
		host_queue = manager.Queue()
		entry_queue = manager.Queue()
		host = Host()
		host_queue.put(host)
		entry_queue.put([])

		# Creating Monitor Process
		logging.info('Starting Monitor Process')
		proc1 = Process(target=process.host_monitor, args=(host_queue,))

		# Creating Request Receiver Process
		logging.info('Starting Request Receiver Process')
		proc2 = Process(target=process.request_receiver, args=(entry_queue,))

		# Creating Container Manager Process
		logging.info('Starting Container Manager Process')
		proc3 = Process(target=process.container_manager, args=(host_queue, entry_queue))

		# Starting Created Processes
		proc1.start()
		proc2.start()
		proc3.start()

		# Adding Processes to a Multiprocessing Pool
		proc1.join()
		proc2.join()
		proc3.join()
