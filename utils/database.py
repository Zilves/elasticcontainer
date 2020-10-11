import dill
import logging
import mysql.connector
from classes.application import Application
from classes.user import User
from classes.request import Request
from configparser import ConfigParser
from datetime import timedelta, datetime

parser = ConfigParser()
parser.read('./config/local-config.txt')

if parser['Container']['type'] == 'DOCKER':
	from classes.container import ContainerDocker as Container

elif parser['Container']['type'] == 'LXC':
	from classes.container import ContainerLXC as Container


# ---------- Database Connection Fuctions ----------
# Function for connect to global database


def get_connection():
	config = ConfigParser()
	config.read('./config/global-config.txt')

	try:
		conn = mysql.connector.connect(host=config['Database']['hostname'],
										database=config['Database']['database'],
										user=config['Database']['user'],
										password=config['Database']['password'])
		return conn

	except mysql.connector.Error as err:
		logging.error('Connection to Database Error: %s', err)


# Function for connect to local database


def get_local_connection():
	config = ConfigParser()
	config.read('./config/local-config.txt')

	try:
		conn = mysql.connector.connect(host=config['Localbase']['hostname'],
										database=config['Localbase']['database'],
										user=config['Localbase']['user'],
										password=config['Localbase']['password'])
		return conn

	except mysql.connector.Error as err:
		logging.error('Connection to Local Database Error: %s', err)


# ---------- Database Publishing Functions ----------
# Function to save host monitoring data on global database


def publish_host(hostname: str, data):
	query = "UPDATE host SET hostdata = %s WHERE hostname = %s"
	info = (data, hostname)

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			conn.commit()
			logging.debug('Host Data %s Updated on Database', hostname)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Publishing Host %s on Database Error: %s', hostname, err)


# Function to save container history monitoring data on global database


def publish_container_history(container):
	query = "INSERT INTO container_history (cid, logdata) VALUES (%s, %s)"

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()

			if not container.state in ['NEW', 'CREATED']:
				serial_container = dill.dumps(container)
				info = (container.cid, serial_container)
				cursor.execute(query, info)

			conn.commit()
			logging.debug('%s Container History Data Inserted on Database', container.name)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Publishing Containers History on Database Error: %s', err)


# Function to save container history monitoring data on local database


def publish_local_container_history(container: Container):
	query = "INSERT INTO container_history (name, data) VALUES (%s, %s)"

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor()

			if not container.state in ['NEW', 'CREATED']:
				serial_container = dill.dumps(container)
				info = (container.name, serial_container)
				cursor.execute(query, info)

			conn.commit()
			logging.debug('%s Container History Data Inserted on Local Database', container.name)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Publishing Containers History on Local Database Error: %s', err)


# ---------- User Table Functions ----------
# Function to create a user on global database


def create_user(user: User):
	query = "INSERT INTO user (login, password, username, usertype) VALUES (%s, password(%s), %s, %s)"
	info = (user.login, user.password, user.name, user.type)

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			conn.commit()
			query = "SELECT LAST_INSERT_ID()"
			cursor.execute(query)
			item = cursor.fetchone()
			uid = item[0]
			logging.info('User %s Created with the ID %s on Database')
			cursor.close()
			conn.close()
			return uid

	except mysql.connector.Error as err:
		logging.error('Creating User %s on Database Error: %s', user.login, err)


# Function for login and password check from a user


def check_login(login: str, password: str):
	query = "SELECT userid FROM user WHERE login = %s AND password = password(%s)"
	#query = "SELECT IF((SELECT password FROM user WHERE login = %s) = password(%s), True, False)"
	info = (login, password)
	uid = None

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			item = cursor.fetchone()

			if item:
				uid = item[0]

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Login %s Check on Database Error: %s', login, err)

	finally:
		if not uid:
			logging.info('User Login %s Not Found', login)
		return uid


# Function to update a user information


def update_user(user: User):
	query = "UPDATE user SET login = %s, password = %s, username = %s, usertype = %s WHERE userid = %s"
	info = (user.login, user.password, user.name, user.type, user.userid)

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			conn.commit()
			print('User %s Updated on Database', user.login)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Update User %s Information on Database Error: %s', user.login, err)


# ---------- Application Table Functions ----------
# Function to create an application on global database


def create_application(app: Application):
	query = "INSERT INTO application (appname, apptype, image, min_memory, num_cores, comments) VALUES (%s, %s, %s, %s, %s, %s)"
	info = (app.name, app.type, app.image, app.min_memory, app.num_cores, app.comments)

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			cursor.commit()
			query = "SELECT LAST_INSERT_ID()"
			cursor.execute(query)
			item = cursor.fetchone()
			appid = item[0]
			logging.info('Application %s Created on Database', app.name)
			cursor.close()
			conn.close()
			return appid

	except mysql.connector.Error as err:
		logging.error('Creating Application %s on Database Error: %s', app.name, err)


# Function to list the stored applications


def list_applications():
	query = "SELECT * FROM application"
	app_list = []

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query)

			for item in cursor:
				app = Application()
				app.appid = item[0]
				app.name = item[1]
				app.type = item[2]
				app.image = item[3]
				app.min_memory = item[4]
				app.num_cores = item[5]
				app.comments = item[6]
				# print('Application: ', vars(app))
				app_list.append(app)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Getting Application List on Database Error: %s', err)

	finally:
		if not app_list:
			logging.info('Not Find any Application on Database')
		return app_list


# Fuction to get an application information


def get_application_from_ID(appid: int):
	query = "SELECT * FROM application WHERE id = %s"
	info = (appid,)
	application = Application()

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			item = cursor.fetchone()

			if item:
				application.appid = item[0]
				application.name = item[1]
				application.type = item[2]
				application.image = item[3]
				application.min_memory = item[4]
				application.num_cores = item[5]
				application.comments = item[6]
				# print('Application: ', vars(application))

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Getting Application %s Info on Database Error: %s', appid, err)

	finally:
		return application


# ---------- Request Table Functions ----------
# Function to create a request on global database


def create_request(request: Request):
	query = "INSERT INTO request (uid, reqname, num_containers) VALUES (%s, %s, %s)"
	info = (request.user, request.name, request.num_containers)

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			conn.commit()
			query = "SELECT LAST_INSERT_ID()"
			cursor.execute(query)
			item = cursor.fetchone()
			reqid = item[0]
			logging.info('Request %s Created on Database', reqid)
			cursor.close()
			conn.close()
			return reqid

	except mysql.connector.Error as err:
		logging.error('Creating the Request %s on Database Error: %s', request.name, err)


# Function to update a request status


def update_request_status(reqid: int, status: str):
	info = (status, reqid)

	if status == 'SCHEDULED':
		query = "UPDATE request SET reqstatus = %s, start_time = CURRENT_TIMESTAMP WHERE reqid = %s"

	elif status in ['FINISHED','ERROR']:
		query = "UPDATE request SET reqstatus = %s, end_time = CURRENT_TIMESTAMP WHERE reqid = %s"

	else:
		query = "UPDATE request SET reqstatus = %s WHERE reqid = %s"

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			conn.commit()
			logging.debug('Request %s Status Updated on Database', reqid)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Updating Status from Request %s on Database Error: %s', reqid, err)


# Function to get new requests from global database


def get_new_requests():
	query = "SELECT reqid, uid, reqname, reqstatus, num_containers FROM request WHERE reqstatus = 'NEW' ORDER BY reqid"
	req_list = []

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query)

			for item in cursor:
				request = Request()
				request.reqid = item[0]
				request.user = item[1]
				request.name = item[2]
				request.status = item[3]
				request.num_containers = item[4]
				print('Request: ', vars(request))
				req_list.append(request)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Getting New Requests on Database Error: %s', err)

	finally:
		if not req_list:
			logging.debug('Not find any New Request on Database')
		return req_list


# ---------- Container Table Functions ----------
# Function to create a container on global database


def create_container(reqid: int, appid: int, name: str, command: str, est_time: timedelta):
	query = "INSERT INTO container (rid, aid, containername, command, estimated_time) VALUES (%s, %s, %s, %s, %s)"
	info = (reqid, appid, name, command, est_time)

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			conn.commit()
			query = "SELECT LAST_INSERT_ID()"
			cursor.execute(query)
			item = cursor.fetchone()
			cid = item[0]
			logging.info('Container %s Created with ID %s on Database', name, cid)
			cursor.close()
			conn.close()
			return cid

	except mysql.connector.Error as err:
		logging.error('Creating Container %s on Database Error: %s', name, err)


# Function to update a container status


def update_container_status(container: Container):
	query = ""
	info = ()

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()

			if container.state == 'RUNNING':
				query = "UPDATE container SET status = %s, start_time = %s WHERE containerid = %s"
				info = (container.state, container.start_time, container.cid)

			elif container.state in ['STOPPED','ERROR']:
				query = "UPDATE container SET status = %s, end_time = CURRENT_TIMESTAMP WHERE containerid = %s"
				info = (container.state, container.cid)

			else:
				query = "UPDATE container SET status = %s WHERE containerid = %s"
				info = (container.state, container.cid)

			cursor.execute(query, info)
			conn.commit()
			logging.info('Container %s Status Updated on Database', container.cid)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Updating Container %s Status on Database Error: %s', container.cid, err)


# Function to get the containers from a particular request


def get_containers_from_request(reqid: int):
	query = "SELECT c.containerid, c.containername, c.command, c.status, c.estimated_time, a.image, a.min_memory, a.max_memory, a.num_cores, a.apptype \
			FROM container c, application a WHERE c.rid = %s AND a.appid = c.aid ORDER BY c.containerid"
	info = (reqid,)
	container_list = []

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)

			for item in cursor:
				container = Container()
				container.cid = item[0]
				container.name = item[1]
				container.command = item[2]
				container.state = item[3]
				container.estimated_time = item[4]
				container.template = item[5]
				container.min_mem_limit = item[6]
				container.max_mem_limit = item[7]
				container.request_cpus = item[8]
				container.apptype = item[9]
				#print('Container: ', vars(container))
				container_list.append(container)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Getting Containers from Request %s on Database Error: %s', reqid, err)

	finally:
		if not container_list:
			logging.debug('Not find any Container from Request %s on Database', reqid)
		return container_list


# Function to get a container stored data from global database


def get_container_history(cid: int):
	query = "SELECT logdata, time FROM container_history WHERE cid = %s"
	info = (cid,)

	time_list = []
	data_list = []

	try:
		conn = get_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				data_list.append(container)
				time_list.append(time)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Database Error: %s', cid, err)

	finally:
		if not data_list and not time_list:
			logging.info('Not Find any Container History for %s on Database', cid)
		return data_list, time_list


def get_local_container_history(name: str):
	query = "SELECT data, time FROM container_history WHERE name = %s"
	info = (name,)

	time_list = []
	data_list = []

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				data_list.append(container)
				time_list.append(time)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)

	finally:
		if not data_list and not time_list:
			logging.info('Not Find any Container History for %s on Local Database', name)
		return data_list, time_list


def get_local_container_history_interval(name, window_length):
	#query = "SELECT data, time FROM container_history WHERE name = %s ORDER BY time DESC LIMIT %s"
	query = "SELECT data, time from container_history WHERE name = %s AND time >= %s ORDER BY time DESC"
	info = (name, window_length)

	time_list = []
	data_list = []

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				data_list.append(container)
				time_list.append(time)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)

	finally:
		if not data_list and not time_list:
			logging.info('Not Find any Container History for %s on Local Database', name)
		return data_list, time_list


# Function to remove a container stored data from local database


def delete_local_container_history(name: str):
	query = "DELETE FROM container_history WHERE name = %s"
	info = (name,)

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor()
			cursor.execute(query, info)
			conn.commit()
			logging.info('Container %s History Deleted from Local Database', name)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)


# ---------- Policies Database Functions ----------


def get_container_memory_consumption(name, window_length):
	query = "SELECT data, time FROM container_history WHERE name = %s ORDER BY time DESC LIMIT %s"
	info = (name, window_length)

	data_list = []
	time_list = []

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor(buffered=True)
			cursor.execute(query, info)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				data_list.append(container)
				time_list.append(time)

			print('Tamanho Datalist = ', len(data_list))
			print('Wall Time: ',(time_list[0] - time_list[-1]).seconds, ' seconds')
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)

	finally:
		delta = 0
		swapdelta = 0

		for index in range(len(data_list) - 1):
			if parser['Container']['type'] == 'LXC':
				delta += data_list[index].getUsedMemory2() - data_list[index + 1].getUsedMemory2()
				swapdelta += int(data_list[index].mem_stats['swap']) - int(data_list[index + 1].mem_stats['swap'])

			elif parser['Container']['type'] == 'DOCKER':
				delta += data_list[index].getUsedMemory() - data_list[index + 1].getUsedMemory()

		print('Delta: ' +  str(delta // 2 ** 20) + 'MB')
		print('Swap Delta: ' +  str(swapdelta // 2 ** 20) + 'MB')
		return delta, swapdelta


def get_container_memory_consumption2(name, window_length):
	query = "SELECT data, time FROM container_history WHERE name = %s ORDER BY time DESC LIMIT %s"
	info = (name, window_length)

	data_list = []
	time_list = []

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor(buffered=True)
			cursor.execute(query, info)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				data_list.append(container)
				time_list.append(time)

			#print('Tamanho Datalist = ', len(data_list))
			walltime = (time_list[0] - time_list[-1]).seconds
			#print('Wall Time: ', walltime, ' seconds')
			logging.info('Container: %s, Datalist Size: %d, Wall Time: %d s', name, len(data_list), walltime)
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)

	finally:
		memory_used = 0
		swap_used = 0
		page_faults = 0
		major_faults = 0

		print('Container: ', name, ' Init Window: ', time_list[0], ' End Window: ', time_list[-1],
			  'Wall: ', (time_list[0] - time_list[-1]).seconds, flush=True)

		for index in range(len(data_list) - 1):
			memory_used += data_list[index].getUsedMemory() - data_list[index + 1].getUsedMemory()
			page_faults += data_list[index].getMemoryPageFaults() - data_list[index + 1].getMemoryPageFaults()
			major_faults += data_list[index].getMemoryMajorFaults() - data_list[index + 1].getMemoryMajorFaults()

			if parser['Container']['type'] == 'LXC':
				swap_used += int(data_list[index].mem_stats['swap']) - int(data_list[index + 1].mem_stats['swap'])

			elif parser['Container']['type'] == 'DOCKER':
				print('Calcular uso de swap no Docker')

		#print('Delta: ' +  str(memory_used // 2 ** 20) + 'MB')
		#print('Swap Delta: ' +  str(swap_used // 2 ** 20) + 'MB')
		#print('Page Faults: ', page_faults)
		#print('Major Faults: ', major_faults)
		logging.info('Container: %s, MemUsed: %s MB, SwapUsed: %s MB, PgFaults: %d, PgMajorFaults: %d',
					name, str(memory_used // 2 ** 20), str(swap_used // 2 ** 20), page_faults, major_faults)

		return {'memory': memory_used, 'swap': swap_used, 'page_faults': page_faults, 'major_faults': major_faults}


def get_container_memory_consumption3(name, last_sched):
	query = "SELECT data, time FROM container_history WHERE name = %s AND time >= %s ORDER BY time DESC"
	info = (name, last_sched)

	data_list = []
	time_list = []

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor(buffered=True)
			cursor.execute(query, info)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				data_list.append(container)
				time_list.append(time)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)

	finally:
		memory_used = 0
		swap_used = 0
		major_faults = 0

		#print('Tamanho Datalist = ', len(data_list)
		walltime = (time_list[0] - time_list[-1]).seconds
		logging.info('Container: %s, Datalist Size: %d, Wall Time: %d s', name, len(data_list), walltime)

		print('Container: ', name, ' Init Window: ', time_list[0], ' End Window: ', time_list[-1],
			  'Wall: ', (time_list[0] - time_list[-1]).seconds, flush=True)

		for index in range(len(data_list) - 1):
			memory_used += (data_list[index].getUsedMemory() - data_list[index + 1].getUsedMemory()) #// (time_list[index] - time_list[index + 1]).seconds
			major_faults += data_list[index].getMemoryMajorFaults() - data_list[index + 1].getMemoryMajorFaults()

			if parser['Container']['type'] == 'LXC':
				swap_used += (int(data_list[index].mem_stats['swap']) - int(data_list[index + 1].mem_stats['swap'])) #// (time_list[index] - time_list[index + 1]).seconds

			elif parser['Container']['type'] == 'DOCKER':
				print('Calcular uso de swap no Docker')

		memory_used = memory_used // walltime
		swap_used = swap_used // walltime

		logging.info('Container: %s, MemUsed: %s MB/s, SwapUsed: %s MB/s, PgMajorFaults: %d',
					name, str(memory_used // 2 ** 20), str(swap_used // 2 ** 20), major_faults)

		return {'memory': memory_used, 'swap': swap_used, 'major_faults': major_faults}


def get_container_memory_consumption4(name, short_sched, long_sched):
	query = "SELECT data, time FROM container_history WHERE name = %s AND time >= %s ORDER BY time"
	info1 = (name, short_sched)
	info2 = (name, long_sched)

	datalist_short = []
	datalist_long = []
	timelist_short = []
	timelist_long = []

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor(buffered=True)
			cursor.execute(query, info1)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				datalist_short.append(container)
				timelist_short.append(time)

			cursor.execute(query, info2)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				datalist_long.append(container)
				timelist_long.append(time)

			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)

	finally:
		memory_used_short = 0
		memory_used_long = 0
		swap_used_short = 0
		swap_used_long = 0
		major_faults = 0

		walltime_long = (timelist_long[-1] - timelist_long[0]).seconds
		walltime_short = (timelist_short[-1] - timelist_short[0]).seconds

		memory_used_short += (datalist_short[-1].getUsedMemory() - datalist_short[0].getUsedMemory()) // walltime_short
		memory_used_long += (datalist_long[-1].getUsedMemory() - datalist_long[0].getUsedMemory()) // walltime_long
		major_faults += datalist_long[-1].getMemoryMajorFaults() - datalist_long[0].getMemoryMajorFaults()

		if parser['Container']['type'] == 'LXC':
			swap_used_short += (int(datalist_short[-1].mem_stats['swap']) - int(datalist_short[0].mem_stats['swap'])) // walltime_short
			swap_used_long += (int(datalist_long[-1].mem_stats['swap']) - int(datalist_long[0].mem_stats['swap'])) // walltime_long

		elif parser['Container']['type'] == 'DOCKER':
			print('Calcular uso de swap no Docker')

		if(memory_used_long + swap_used_long) <= (memory_used_short + swap_used_short):
			memory_used = memory_used_short
			swap_used = swap_used_short

		else:
			memory_used = (memory_used_long + memory_used_short) / 2
			swap_used = (swap_used_long + swap_used_short) / 2

		logging.info('Container: %s, MemUsed: %s MB/s, SwapUsed: %s MB/s, PgMajorFaults: %d',
					name, str(memory_used // 2 ** 20), str(swap_used // 2 ** 20), major_faults)

		return {'memory': memory_used, 'swap': swap_used, 'major_faults': major_faults}


def get_container_memory_consumption_ED(name, window_length):
	query = "SELECT data, time FROM container_history WHERE name = %s ORDER BY time DESC LIMIT %s"
	info = (name, window_length)

	data_list = []
	time_list = []

	try:
		conn = get_local_connection()

		if conn:
			cursor = conn.cursor(buffered=True)
			cursor.execute(query, info)

			for item in cursor:
				container = dill.loads(item[0])
				time = item[1]
				data_list.append(container)
				time_list.append(time)

			print('Tamanho Datalist = ', len(data_list))
			print('Wall Time: ',(time_list[0] - time_list[-1]).seconds, ' seconds')
			cursor.close()
			conn.close()

	except mysql.connector.Error as err:
		logging.error('Get Container %s History on Local Database Error: %s', name, err)

	finally:
		delta = 0

		for index in range(len(data_list)):
			if (index % 2) == 0:
				if parser['Container']['type'] == 'LXC':
					delta += data_list[index].getUsedMemory2()

				elif parser['Container']['type'] == 'DOCKER':
					delta += data_list[index].getUsedMemory()

		media = delta // (len(data_list) // 2)

		print('Media: ' +  str(media // 2 ** 20) + 'MB')
		return media
