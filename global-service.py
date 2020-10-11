#!/usr/bin/env python3

import logging
from utils import process
import multiprocessing as mp

if __name__ == '__main__':
	#logging.basicConfig(filename='./log/global-service.log', filemode='w', format='%(asctime)s %(levelname)s:%(message)s',
	#					datefmt='%d/%m/%Y %H:%M:%S',level=logging.INFO)

	logGS = logging.getLogger(__name__)
	logGS.setLevel(logging.INFO)
	format = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
	file_handler = logging.FileHandler(filename = './log/global-service.log', mode = 'a')
	file_handler.setFormatter(format)
	file_handler.setLevel(logging.INFO)
	logGS.addHandler(file_handler)

	ctx = mp.get_context('spawn')

	with ctx.Manager() as manager:
		# Creating Host and Request Lists
		host_list = manager.list()
		request_list = manager.list()

		# Creating Global Monitor Process
		logGS.info('Starting Monitor Manager Process')
		proc1 = ctx.Process(target=process.global_monitor, args=(host_list, request_list))

		# Creating Global Scheduler Process
		logGS.info('Starting Request Scheduler Process')
		proc2 = ctx.Process(target=process.global_scheduler, args=(host_list, request_list))

		# Starting Created Processes
		proc1.start()
		proc2.start()

		# Adding Process to a Multiprocessing Pool
		proc1.join()
		proc2.join()
