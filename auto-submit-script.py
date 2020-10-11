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
appid = 4
req_name = 'J2 4GB 4L 4B Rqst%'
command = '/opt/jobs/memorybound 4096 4 4 2'
num_requests = 1
num_containers = 1
num_reps = 1
estimated_time = 500
rqst_interval = 60
reps_interval = 500

if __name__ == '__main__':
    logging.basicConfig(filename='./log/auto-submit-script.log', filemode='a', format='%(asctime)s %(levelname)s:%(message)s',
						datefmt='%d/%m/%Y %H:%M:%S',level=logging.INFO)

    logging.info('Start Submission Test:')
    for i in range(num_reps):
        logging.info('Test %d', (i + 1))

        for j in range(num_requests):
            #Creating Request
            request = Request()
            request.name = req_name.replace('%', str(j + 1))
            request.user = uid
            request.num_containers = num_containers
            reqid = database.create_request(request)
            logging.info('Request Created: %d', reqid)

            #Creating Container
            for k in range(num_containers):
                container = Container()
                container.name = 'rqst' + str(reqid) + 'cntnr' + str(k)
                container.appid = appid
                container.command = command
                container.estimated_time = timedelta(seconds = estimated_time)
                request.listcontainers.append(container)
                database.create_container(reqid, container.appid, container.name, container.command, container.estimated_time)
                logging.info('Container Submitted: %s', container.name)

            if j < (num_requests - 1):
                time.sleep(rqst_interval)

        time.sleep(reps_interval)
    logging.info('End of the Test')
