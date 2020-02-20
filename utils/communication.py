import dill
import socket
import struct
import logging
from configparser import ConfigParser
from multiprocessing import Queue


# ---------- Monitors Communication ----------
# Function to send monitoring data to host manager


def send_monitor_data(host):
	config = ConfigParser()
	config.read('./config/local-config.txt')
	conn_address = (config['Manager']['global_ip'], int(config['Manager']['global_send_port']))

	serial_host = dill.dumps(host)
	# print('Host Send Size: ', sys.getsizeof(serial_host))
	message = struct.pack('>I', len(serial_host)) + serial_host

	try:
		host_socket = socket.socket()
		host_socket.connect(conn_address)
		host_socket.sendall(message)
		# print('Data Sended with Success!')
		host_socket.close()

	except socket.error as err:
		logging.error('Sending Monitor Data to Global Error: %s', err)


# Function to receive monitoring data from hosts


def receive_monitor_data():
	config = ConfigParser()
	config.read('./config/global-config.txt')
	#conn_address = (config['Manager']['ip'], int(config['Manager']['port']))

	data = b''
	host = None

	try:
		global_socket = socket.socket()
		#global_socket.bind((conn_address[0], conn_address[1]))
		global_socket.bind((config['Manager']['global_ip'], int(config['Manager']['global_receive_port'])))
		global_socket.listen(5)
		receive_socket, address = global_socket.accept()
		#print('Connection from: %s' % str(address))
		message_length = recvall(receive_socket, 4)

		if not message_length:
			logging.error("Received Empty Message")

		message = struct.unpack('>I', message_length)[0]
		data = recvall(receive_socket, message)
		# print('Received Host Size: ', sys.getsizeof(data))
		# print('Data Received with Success!')
		host = dill.loads(data)
		global_socket.close()

	except socket.error as err:
		logging.error('Receiving Monitor Data Error: %s', err)

	finally:
		return data, host


# ---------- Container Request Communication ----------
# Function to send a container request to a particular host


#def send_container_request(request, address):
def send_container_request(request, hostname):
	config = ConfigParser()
	config.read('./config/global-config.txt')
	address = (hostname, int(config['Manager']['default_send_port']))

	# print('Request: ', request)
	serial_request = dill.dumps(request)
	# print('Request Send Size: ', sys.getsizeof(serial_request))

	try:
		global_socket = socket.socket()
		global_socket.connect(address)
		global_socket.send(serial_request)
		# print('Request Sended with Success!')
		global_socket.close()

	except socket.error as err:
		logging.error('Sending Request to Host %s Error: %s', address[0], err)


# Function to receive container request from global scheduler


def receive_container_request():
	config = ConfigParser()
	config.read('./config/local-config.txt')

	data = b''
	request = None

	try:
		host_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		host_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		host_socket.bind((socket.gethostname(), int(config['Manager']['local_receive_port'])))
		host_socket.listen(5)
		receive_socket, address = host_socket.accept()
		# print('Connection from: %s' % str(address))
		data = receive_socket.recv(1024)
		# print('Received Request Size: ', sys.getsizeof(data))
		# print('Request Received with Success!')
		request = dill.loads(data)
		# print('Received Request: ', request)
		host_socket.close()

	except socket.error as err:
		logging.error('Receiving Request from Global Error: %s', err)

	finally:
		return request


# Generic Function to help with serialized data


def recvall(temp_socket, n):
	data = b''

	while len(data) < n:
		packet = temp_socket.recv(n - len(data))

		if not packet:
			logging.error('Empty Packet')
			return None

		data += packet

	return data


# Conn Thread

def receive_thread(connection, entry_queue: Queue):
	data = b''
	container = None
	config = ConfigParser()
	config.read('./config/local-config.txt')

	data = connection.recv(1024)
	container = dill.loads(data)
	connection.close()

	if container:
		logging.info('Received New Container %s and Added to Entry List', container.name)
		logging.debug('New Container: %s', vars(container))

		if config['Container']['type'] == 'LXC':
			container.createContainer()

	entry_list = entry_queue.get()
	entry_list.append(container)
	entry_queue.put(entry_list)
