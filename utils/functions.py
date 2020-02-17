import logging
from utils import database
from utils import communication
from classes.container import Container
from configparser import ConfigParser


config = ConfigParser()
config.read('../config/global-config.txt')


# Request Functions


def request_bin_packing(host_list, used_hosts, request, app):
    total_cpu_needed = request.num_containers * app.num_cores
    total_memory_needed = request.num_containers * app.min_memory
    found = False
    index = 0

    while not found and (index < len(host_list)):
        host = host_list[index]
        available_cores = host.get_available_cores()
        available_memory = host.memory.available
        used = next(item for item in used_hosts if item['name'] == host.hostname)

        if used:
            available_cores -= used['cores']
            available_memory -= used['memory']

        if (total_cpu_needed < available_cores) and (total_memory_needed < available_memory):
            for i in range(request.num_containers):
                name = 'rqst' + (str)(request.reqid) + 'cntnr' + (str)(i)
                database.create_container(name, request.reqid)
                container = Container(name, app.container_image, request.command, app.min_memory, app.num_cores)
                request.listcontainers.append(container)
                communication.send_container_request(container, (host.hostname, 8800))
                logging.info('Sending Container %s to Host %s', container.name, host.hostname)

            if used:
                used['cores'] += total_cpu_needed
                used['memory'] += total_memory_needed

            else:
                used_hosts.append({'name':host.hostname, 'cores':total_cpu_needed, 'memory':total_memory_needed})

            found = True

        else:
            index += 1

    return found


def request_round_robin(host_list, used_hosts, request, app): # Em dev
    index = 0

    for i in range(request.num_containers):
        host = host_list[index]
        available_cores = host.get_available_cores()
        available_memory = host.memory.available
        used = next(item for item in used_hosts if item['name'] == host.hostname)

        if used:
            available_cores -= used['cores']
            available_memory -= used['memory']

        if(app.num_cores < available_cores) and (app.min_memory < available_memory):
            name = 'rqst' + (str)(request.reqid) + 'cntnr' + (str)(i)
            database.create_container(name, request.reqid)
            container = Container(name, app.container_image, request.command, app.min_memory, app.num_cores)
            request.listcontainers.append(container)
            communication.send_container_request(container, (host.hostname, 8800))
            logging.info('Sending Container %s to Host %s', container.name, host.hostname)
