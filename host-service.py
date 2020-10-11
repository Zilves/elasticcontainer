#!/usr/bin/env python3
import logging
from utils import process
import multiprocessing as mp

if __name__ == '__main__':
	#logging.basicConfig(filename='./log/host-service2.log', filemode='a', format='%(asctime)s %(levelname)s:%(message)s',
	#					datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)
	logHS = logging.getLogger('Host_Service')
	logHS.setLevel(logging.INFO)
	format = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
	file_handler = logging.FileHandler(filename = './log/host-service2.log', mode = 'a')
	file_handler.setFormatter(format)
	file_handler.setLevel(logging.INFO)
	logHS.addHandler(file_handler)

	#mp.set_start_method('spawn')
	ctx = mp.get_context('spawn')

	with ctx.Manager() as manager:
		shared_list = manager.list()
		ac_list = []
		ic_list = []
		core_list = []
		shared_list.append(ac_list)
		shared_list.append(ic_list)
		shared_list.append(core_list)
		#print(shared_list)
		entry_queue = manager.Queue()

		# Creating Monitor Process
		logHS.info('Starting Monitor Process')
		proc1 = ctx.Process(target=process.host_monitor, args=(shared_list,))

		# Creating Request Receiver Process
		logHS.info('Starting Request Receiver Process')
		proc2 = ctx.Process(target=process.request_receiver, args=(entry_queue,))

		# Creating Container Manager Process
		logHS.info('Starting Container Manager Process')
		proc3 = ctx.Process(target=process.no_manager, args=(shared_list, entry_queue))
		#proc3 = ctx.Process(target=process.container_manager2, args=(shared_list, entry_queue))

		# Starting Created Processes
		proc1.start()
		proc2.start()
		proc3.start()

		# Adding Processes to a Multiprocessing Pool
		proc1.join()
		proc2.join()
		proc3.join()
