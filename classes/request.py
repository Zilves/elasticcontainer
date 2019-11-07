class Request:


### Constructor for request class


	def __init__(self):
		self.reqid = -1
		self.user = -1
		self.name = ''
		self.status = 'NEW'
		self.num_containers = 0
		self.listcontainers = []


### Function to verify an equality of two requests


	def __eq__(self, other):
		if isinstance(other, self.__class__):

			if self.reqid == other.reqid:
				return True

			else:
				return False


### Functions to print requests


	def __str__(self):
		out = []

		for key in self.__dict__:
			out.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

		return ', '.join(out)


	def __repr__(self):
		return str(self.reqid) + ' ' + self.status


### Function to update container status from a request


	def check_container_status(self, container_list):
		if self.status == 'SCHEDULED':
			for container in self.listcontainers:
				index1 = self.listcontainers.index(container)

				if container in container_list:
					index2 = container_list.index(container)
					self.listcontainers[index1] = container_list[index2]

		elif self.status == 'RUNNING':
			for container in self.listcontainers:
				index1 = self.listcontainers.index(container)

				if container in container_list:
					index2 = container_list.index(container)
					self.listcontainers[index1] = container_list[index2]
				else:
					self.listcontainers.remove(container)


### Function to change a request status based upon the container status


	def change_status(self):
		check = False

		# if any(container.state == 'RUNNING' for container in self.listcontainers):
		if (self.status == 'SCHEDULED') and (any(container.state == 'RUNNING' for container in self.listcontainers)):
			self.status = 'RUNNING'
			check = True
		elif (not self.listcontainers) and (self.status == 'RUNNING'):
			self.status = 'FINISHED'
			check = True

		return check
