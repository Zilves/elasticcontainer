import logging
import database
import communication
from configparser import ConfigParser


parser = ConfigParser()
parser.read('./config/global-config.txt')

if parser['Container']['type'] == 'DOCKER':
	from classes.container import ContainerDocker as Container

elif parser['Container']['type'] == 'LXC':
	from classes.container import ContainerLXC as Container


# ---------- Escalonador Base para Envio de Requisições ----------


def one_host_global_scheduler(host_list, req_list, new_list):
	if host_list:
		host = host_list[0]

		for request in new_list:
			request.listcontainers = database.get_containers_from_request(request.reqid)
			print(request)

			for container in request.listcontainers:
				communication.send_container_request(container, host.hostname)
				logging.info('Sending Container %s to Host %s', container.name, host.hostname)
				#time.sleep(1)

			if request.status == 'QUEUED':
				request.status = 'SCHEDULED'
				database.update_request_status(request.reqid, request.status)
				req_list.append(request)
				new_list.remove(request)
	else:
		logging.debug('No Hosts Available for Scheduling')
