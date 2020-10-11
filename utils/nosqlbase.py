import logging
from pymongo import MongoClient
from configparser import ConfigParser
import jsonpickle
from datetime import datetime

parser = ConfigParser()
parser.read('./config/local-config.txt')

if parser['Container']['type'] == 'DOCKER':
	from classes.container import ContainerDocker as Container

elif parser['Container']['type'] == 'LXC':
	from classes.container import ContainerLXC as Container


def get_connection():
    config = ConfigParser()
    config.read('./config/local-config.txt')

    try:
        connection = MongoClient('localhost', 27017)
        db = connection['localbase']
        return db

    except Exception as err:
        logging.error('Connection to Database Error: %s', err)


def publish_container_history(container: Container):

	try:
		db = get_connection()
		history = db[container.name]
		container_dict = {'data': jsonpickle.encode(container), 'timestamp': datetime.now()}
		id = history.insert(container_dict)
        #print(id)
		logging.debug('%s Container History Data Inserted on Database with ID %s', container.name, id)

	except Exception as err:
		logging.error('Database Error: %s', err)


def get_container_history(name):

	try:
		db = get_connection()
		history = db[name]
		cursor = history.find()

		data = []
		time = []

		for item in cursor:
			time.append(item['timestamp'])
			container = jsonpickle.decode(item['data'])
			data.append(container)

		return data, time

	except Exception as err:
		logging.error('Database Error: %s', err)


def get_container_history_interval(name, interval):

	try:
		db = get_connection()
		history = db[name]
		cursor = history.find({'timestamp':{'$gte':interval}})

		data = []
		time = []

		for item in cursor:
			time.append(item['timestamp'])
			container = jsonpickle.decode(item['data'])
			data.append(container)

		return data, time

	except Exception as err:
		logging.error('Database Error: %s', err)
