#!/usr/bin/env python3

import logging
from utils import process
from multiprocessing import Process, Manager

if __name__ == '__main__':
	logging.basicConfig(filename='./log/global-service.log', filemode='w', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S',level=logging.INFO)

	with Manager() as manager:
		# Creating Host and Request Lists
		host_list = manager.list()
		request_list = manager.list()

		# Creating Global Monitor Process
		logging.info('Starting Monitor Manager Process')
		proc1 = Process(target=process.global_monitor, args=(host_list, request_list))

		# Creating Global Scheduler Process
		logging.info('Starting Request Scheduler Process')
		proc2 = Process(target=process.global_scheduler, args=(host_list, request_list))

		# Starting Created Processes
		proc1.start()
		proc2.start()

		# Adding Process to a Multiprocessing Pool
		proc1.join()
		proc2.join()
