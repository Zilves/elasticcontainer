#!/usr/bin/env python3
import sys
import getpass
import logging
import time
from datetime import timedelta
from utils import database
from classes.request import Request
from classes.user import User
from classes.container import Container
from classes.application import Application

uid = 4
appid = 2
req_name = 'J1 4GB 4L T%'
command = '/opt/jobs/memorybound 4096 4 1 1'
num_containers = 1
num_reps = 1
estimated_time = 1000
waiting_time = 500

if __name__ == '__main__':
    logging.basicConfig(filename='./log/auto-submit-script.log', filemode='w', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S',level=logging.INFO)

    logging.info('Start Submission Test:')
    for ind in range(num_reps):
        logging.info('Test %d', (ind + 1))

        #Creating Request
        request = Request()
        request.name = req_name.replace('%', str(ind + 1))
        request.user = uid
        request.num_containers = 1
        reqid = database.create_request(request)

        #Creating Container
        for ind2 in range(num_containers):
            container = Container()
            container.name = 'rqst' + str(reqid) + 'cntnr' + str(ind2)
            container.appid = appid
            container.command = command
            container.estimated_time = timedelta(seconds = estimated_time)
            request.listcontainers.append(container)
            database.create_container(reqid, container.appid, container.name, container.command, container.estimated_time)

        time.sleep(waiting_time)
