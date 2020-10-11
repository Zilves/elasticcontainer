import mmap
import psutil
import logging
from datetime import datetime, timedelta
from utils import nosqlbase
from classes.host import Host
from configparser import ConfigParser
from threading import Thread
import multiprocessing as mp

parser = ConfigParser()
parser.read('./config/local-config.txt')

if parser['Container']['type'] == 'DOCKER':
	from classes.container import ContainerDocker as Container

elif parser['Container']['type'] == 'LXC':
	from classes.container import ContainerLXC as Container

log_vmc = logging.getLogger('Container_Manager.VEMOC')

class VEMOC:

	def __init__(self):
		self.long_interval = 0
		self.short_interval = 0
		self.sched_interval = 0
		self.MUE = 0
		self.mem_write_rate = 0
		self.swapout_rate = 0
		self.swapin_rate = 0
		self.spare_mem_cap = 0
		self.latency = 0
		self.sched_start_time = 0
		self.NAHM = 0
		self.MTMPF = 0
		self.mem_test = 0
		self.steal_check = False
		self.need_list = []
		self.urgent_list = []
		self.provider_list = []
		self.memory_needed = 0
		self.memory_urgent = 0
		self.pause_demand = 0
		self.pause_count = 0


	def reset(self):
		self.spare_mem_cap = 0
		self.NAHM = 0
		self.steal_check = False
		self.need_list = []
		self.urgent_list = []
		self.provider_list = []
		self.memory_needed = 0
		self.memory_urgent = 0
		self.pause_demand = 0
		self.pause_count = 0


	# Functions get and set:


	def getNAHM(self):
		return self.NAHM


	def setNAHM(self, nahm):
		self.NAHM = nahm


	def getMUE(self):
		return self.MUE


	def setMUE(self, mue):
		self.MUE = mue


	def getMemoryNeeded(self):
		return self.memory_needed


	def getMemoryUrgent(self):
		return self.memory_urgent


	def getPauseDemand(self):
		return self.pause_demand


	def getStealCheck(self):
		return self.steal_check


	def getTotalMemoryDemand(self):
		result = self.memory_needed + self.memory_urgent + self.pause_demand
		return result


	# Scheduler Functions:


	def get_memory_classification2(self, container: Container, consumption): # Algorithm 4
		mem_state = container.getMemoryState()

		if consumption['has_mpf'] == True:
			#if(mem_state == 'STABLE') and (container.mem_repo == True):
			if container.mem_repo == True:
				container.repo_SI = True

			if mem_state != 'RISING':
				container.setMemoryState('RISING')
				container.mem_state_time = datetime.now()

		elif (consumption['pgin'] == 0) and (consumption['pgout'] == 0):

			if mem_state != 'STABLE':
				container.setMemoryState('STABLE')
				container.mem_state_time = datetime.now()

		elif (consumption['pgout'] >= consumption['pgin']):

			if consumption['swap'] == 0:

				if(container.block_repo == True):
					container.block_repo = False

				if mem_state != 'FALLING':
					container.setMemoryState('FALLING')
					container.mem_state_time = datetime.now()

			elif mem_state != 'STABLE':
				container.setMemoryState('STABLE')
				container.mem_state_time = datetime.now()

		else:

			if(container.block_repo == True):
				container.block_repo = False

			if mem_state != 'RISING':
				container.setMemoryState('RISING')
				container.mem_state_time = datetime.now()

		return container.getMemoryState(), container.getMemoryStateTime()


	def get_memory_consumption_rate(self, container:Container, MTMPF): # Algorithm 3
		pgvar = 33
		instant_time = datetime.now()
		#long_time = instant_time - timedelta(seconds=(self.long_interval + 1))
		short_time = instant_time - timedelta(seconds=self.short_interval)
		#datalist_long, timelist_long = nosqlbase.get_container_history_interval(container_name, long_time)
		datalist_long, timelist_long = nosqlbase.get_container_history_interval(container.name, container.last_data_analyzed)
		datalist_short, timelist_short = nosqlbase.get_container_history_interval(container.name, short_time)

		if (len(timelist_short) > 0) and (len(timelist_long) > 0):
			md_latency = (self.sched_start_time - min(timelist_long[-1], timelist_short[-1])).total_seconds()

		elif(len(timelist_long) > 0):
			md_latency = (self.sched_start_time - timelist_long[-1]).total_seconds()

		else:
			md_latency = (self.sched_start_time - datetime.now()).total_seconds()

		if (len(datalist_long) > 1) and (len(timelist_long) > 1):
			log_vmc.info('C: %s, DL length: %d, TL[0]: %s, TL[-1]: %s', container.name, len(datalist_long),
						timelist_long[0], timelist_long[-1])
			walltime_long = (timelist_long[-1] - timelist_long[-0]).total_seconds()
			major_faults = datalist_long[-1].getMemoryMajorFaults() - datalist_long[0].getMemoryMajorFaults()
			#swap_long = int((datalist_long[-1].getUsedSwap() - datalist_long[0].getUsedSwap()) / walltime_long)
			swap_long = round((datalist_long[-1].getUsedSwapPG() - datalist_long[0].getUsedSwapPG()) / walltime_long)
			pgin_long = round((int(datalist_long[-1].mem_stats['pgpgin']) - int(datalist_long[0].mem_stats['pgpgin'])) / walltime_long)
			pgout_long = round((int(datalist_long[-1].mem_stats['pgpgout']) - int(datalist_long[0].mem_stats['pgpgout'])) / walltime_long)
			log_vmc.info('C: %s, Walltime_Long: %f, Swap Long: %d, PgIN_Long: %d, PgOUT_Long: %d, MjFault: %d', container.name, walltime_long,
						swap_long, pgin_long, pgout_long, major_faults)

		else:
			walltime_long = 0
			swap_long = 0
			major_faults = 0
			pgin_long = 0
			pgout_long = 0
			log_vmc.info('C: %s does not have enough data (<=1) to calculate rates', container.name)

		if len(datalist_long) > 1:
			container.last_data_analyzed = timelist_long[-1]

		else:
			container.last_data_analyzed = datetime.now()

		if (len(datalist_short) > 1) and (len(timelist_short) > 1):
			log_vmc.info('C: %s, DS length: %d, TS[0]: %s, TS[-1]: %s', container.name, len(datalist_short),
						timelist_short[0], timelist_short[-1])
			walltime_short = (timelist_short[-1] - timelist_short[0]).total_seconds()
			#swap_short = int((datalist_short[-1].getUsedSwap() - datalist_short[0].getUsedSwap()) / walltime_short)
			swap_short = round((datalist_short[-1].getUsedSwapPG() - datalist_short[0].getUsedSwapPG()) / walltime_short)
			pgin_short = round((int(datalist_short[-1].mem_stats['pgpgin']) - int(datalist_short[0].mem_stats['pgpgin'])) / walltime_short)
			pgout_short = round((int(datalist_short[-1].mem_stats['pgpgout']) - int(datalist_short[0].mem_stats['pgpgout'])) / walltime_short)
			log_vmc.info('C: %s, Walltime_Short: %f, Swap Short: %d, PgIN_Short: %d, PgOUT_Short: %d', container.name, walltime_short,
						swap_short, pgin_short, pgout_short)

		else:
			walltime_short = 0
			swap_short = 0
			pgin_short = 0
			pgout_short = 0
			log_vmc.info('C: %s does not have enough data (<=1) to calculate rates', container.name)

		if major_faults <= MTMPF:
			faults = False

		else:
			faults = True

		if pgin_long > 0:

			if pgin_long <= pgin_short:
				consumption = pgin_short + pgvar

			else:
				consumption = round((pgin_long + pgin_short) / 2) + pgvar

		else:
			consumption = 0

		pgin_long = pgin_long >> 6
		pgout_long = pgout_long >> 6

		return {'swap': swap_long, 'has_mpf': faults, 'md_latency': md_latency, 'pgin': pgin_long, 'pgout': pgout_long, 'consumption': int(consumption)}


	def mem_demand_estimation2(self, host: Host): # Algorithm 2
		self.mem_test = 2000
		self.MTMPF = 33

		for container in host.container_active_list:

			if container.getContainerState() == 'RUNNING':
				log_vmc.info('---------------------------------------------------------')
				consumption = self.get_memory_consumption_rate(container, self.MTMPF)
				mem_state, mem_state_time = self.get_memory_classification2(container, consumption)
				mem_limit = container.getMemoryLimitPG()
				max_mem_limit = container.getMaxMemoryLimitPG()
				min_mem_limit = container.getMinMemoryLimitPG()
				mem_used = container.getUsedMemoryPG()

				log_vmc.info('Container: %s, Running_time: %f, MS: %s, MST: %f, MDLat: %f', container.name,
							container.getRunningTime(), mem_state, mem_state_time, consumption['md_latency'])
				log_vmc.info('Container: %s, CMU: %d, CML: %d', container.name, mem_used, mem_limit)
				log_vmc.info('Container: %s, SU: %d, MC: %d', container.name, consumption['swap'], consumption['consumption'])
				log_vmc.info('Container: %s, PgIn: %d, PgOut: %d, MJF: %s', container.name, consumption['pgin'],
							consumption['pgout'], consumption['has_mpf'])

				if mem_state == 'RISING':
					delta = round(consumption['consumption'] * (self.sched_interval + consumption['md_latency'] + self.latency))

					if mem_state_time < self.sched_interval:
						delta = max(delta, self.spare_mem_cap)

					delta_lim = round((mem_used + delta) / self.MUE - mem_limit + 1)

					if delta_lim > 0:

						if (mem_limit + delta_lim) > max_mem_limit:
							delta_lim = max_mem_limit - mem_limit

						if consumption['has_mpf'] == True:
							container.setDeltaMemory(delta_lim)
							container.mem_delta_check = False
							self.urgent_list.append(container)
							self.memory_urgent += delta_lim
							log_vmc.info('Urgent Container (RISING): %s, Using: %d, Delta: %d, Limit: %d', container.name, mem_used, delta_lim, mem_limit)

						else:
							container.setDeltaMemory(delta_lim)
							container.mem_delta_check = False
							self.need_list.append(container)
							self.memory_needed += delta_lim
							log_vmc.info('Need Container (RISING): %s, Using: %d, Delta: %d, Limit: %d', container.name, mem_used, delta_lim, mem_limit)

					else:
						container.setDeltaMemory(delta)
						container.mem_delta_check = False
						self.provider_list.append(container)
						log_vmc.info('Provider Container (RISING): %s, Delta, %d, Using: %d, Limit: %d', container.name, delta, mem_used, mem_limit)

				elif mem_state == 'STABLE':
					log_vmc.info('Stable T1: Container: %s, block_repo: %s, repo_SI: %s', container.name, container.block_repo, container.repo_SI)

					if (mem_state_time < self.sched_interval / 2) and (container.repo_SI == True):
						container.block_repo = True
						container.repo_SI = False
						diff = (mem_used - container.mem_used_pre_repo) >> 6
						log_vmc.info('Container %s Block Repo FALSE -> TRUE', container.name)

						if diff >= 0:
							container.repo_lim = container.mem_used_pre_repo
							container.mem_repo = False

					log_vmc.info('Stable T2: Container: %s, block_repo: %s', container.name, container.block_repo)

					if (container.block_repo == False) and (mem_state_time < self.sched_interval / 2):
						self.NAHM = round(self.NAHM + mem_limit - (mem_used - self.mem_test) / self.MUE)
						mem_limit = round((mem_used - self.mem_test) / self.MUE)

						if mem_limit < min_mem_limit:
							self.NAHM = self.NAHM - (min_mem_limit - mem_limit)
							mem_limit = min_mem_limit

						container.mem_used_pre_repo = mem_used
						container.mem_repo = True
						container.last_repo = mem_state_time
						container.update_LRU = True

						container.setMemLimit2(mem_limit)
						log_vmc.info('Limit Test: %s, new CML T0\u25BC: %d, new NAHM\u25B2: %d', container.name, container.getMemoryLimitPG(), self.NAHM)

					elif ((container.block_repo == True) and (mem_state_time < self.sched_interval)) or (container.update_LRU == True):
						log_vmc.info('Stable T3: Container: %s, block_repo: %s, update_LRU: %s', container.name, container.block_repo, container.update_LRU)
						container.update_LRU = False
						delta_lim = round((mem_used + self.spare_mem_cap) / self.MUE - mem_limit)

						if delta_lim > 0:

							if (mem_limit + delta_lim) > max_mem_limit:
								delta_lim = max_mem_limit - mem_limit

							container.setDeltaMemory(delta_lim)
							container.mem_delta_check = False
							self.need_list.append(container)
							self.memory_needed += delta_lim
							log_vmc.info('Need Container (STABLE): %s, Using: %d, Delta: %d, Limit: %d', container.name, mem_used, delta_lim, mem_limit)

						else:
							self.provider_list.append(container)
							log_vmc.info('Provider Container (STABLE/Delta < 0): %s, Using: %d, Limit: %d', container.name, mem_used, mem_limit)

					else:
						self.provider_list.append(container)
						log_vmc.info('Provider Container (STABLE/ block_repo Else): %s, Using: %d, Limit: %d', container.name, mem_used, mem_limit)

				else:
					self.provider_list.append(container)
					log_vmc.info('Provider Container (FALLING): %s, Using: %d, Limit: %d', container.name, mem_used, mem_limit)

			elif container.getContainerState() == 'PAUSED':
				self.pause_demand += container.mem_delta
				self.pause_count += 1


	def passive_memory_reduction2(self): # Algorithm 5 NEW

		for container in self.provider_list:
			delta_mem = 0
			mem_used = container.getUsedMemoryPG()
			mem_limit = container.getMemoryLimitPG()
			min_mem_limit = container.getMinMemoryLimitPG()
			spare_mem = self.spare_mem_cap * 0.8

			if container.getMemoryState() == 'RISING':
				delta_mem = container.getDeltaMemory()
				#spare_mem = 0

			#else:
			#	spare_mem = self.spare_mem_cap

			if (mem_used + delta_mem) < ((mem_limit * self.MUE) - spare_mem):
				reduction = round(mem_limit - (mem_used + delta_mem + spare_mem) / self.MUE)
				mem_limit -= reduction
				self.NAHM += reduction

				if mem_limit < min_mem_limit:
					self.NAHM = self.NAHM - (min_mem_limit - mem_limit)
					mem_limit = min_mem_limit

				log_vmc.info('C: %s, Delta Reduction: %d, Mem Limit: %d', container.name, reduction, mem_limit)

				container.setMemLimit2(mem_limit)
				log_vmc.info('Passive Provider: %s, Delta: %d, new CML T1\u25BC: %d, new NAHM\u25B2: %d', container.name,
							reduction, container.getMemoryLimitPG(), self.NAHM)


	def active_memory_recovery3(self): # Algorithm 6
		repo_interval = 50
		mem_req = self.memory_needed + self.memory_urgent + self.pause_demand
		log_vmc.info('Total Memory Requested: %d', mem_req)
		mem_repo_thd = 256

		self.provider_list.sort(key=lambda container: container.getMemoryStateTime(), reverse=True)
		index = 0

		while((mem_req - self.NAHM) > 0) and (index < len(self.provider_list)):
		#while index < len(self.provider_list):
			container = self.provider_list[index]
			log_vmc.info('C: %s, Repo Limit: %d, Bloco Repo: %s', container.name, container.repo_lim, container.block_repo)

			if (container.getMemoryState() == 'STABLE') and (container.block_repo == False) and (container.getUsedMemoryPG() > container.repo_lim):
				beta = container.getInactiveMemoryPG()
				mem_used = container.getUsedMemoryPG()
				mem_limit = container.getMemoryLimitPG()
				min_mem_limit = container.getMinMemoryLimitPG()
				mem_state_time = container.getMemoryStateTime()

				log_vmc.info('C: %s, Memory Inactive: %d', container.name, beta)

				if beta > mem_repo_thd:
					delta_rec = min((self.swapout_rate * self.sched_interval), (mem_req - self.NAHM))
					#delta_rec = (self.swapout_rate * self.sched_interval)
					#delta_rec = min((self.mem_write_rate * self.sched_interval), (mem_req - self.NAHM))
					#delta_rec = (self.mem_write_rate * self.sched_interval)

					if beta > delta_rec:
						beta = delta_rec

					log_vmc.info('C: %s, Beta: %d', container.name, beta)
					#beta = round(beta / 2)

					#self.NAHM = round(self.NAHM + mem_limit - (mem_used - beta) / self.MUE)
					self.NAHM = round(self.NAHM + beta)
					#mem_limit = round((mem_used - beta) / self.MUE)
					mem_limit = round(mem_used - beta)
					self.steal_check = True
					container.last_repo = mem_state_time
					container.mem_repo = True
					#container.block_repo = True

					if (mem_limit * self.MUE) < container.repo_lim:
						self.NAHM = round(self.NAHM - (container.repo_lim - mem_limit * self.MUE))
						mem_limit = round(container.repo_lim / self.MUE)

					diff = container.getMemoryLimitPG() - mem_limit
					#mem_req -= diff

					mp.Process(target = container.setMemLimit2, args=(mem_limit,)).start()
					log_vmc.info('Provider T1: %s, delta_repo: %d, new CML T2\u25BC: %d, new NAHM\u25B2: %d', container.name, diff, mem_limit, self.NAHM)

				elif mem_state_time > (container.last_repo + self.sched_interval * repo_interval):
					self.NAHM = round(self.NAHM + mem_limit - (mem_used + self.mem_test) / self.MUE)
					mem_limit = round((mem_used + self.mem_test) / self.MUE)
					container.update_LRU = True

					if mem_limit < min_mem_limit:
						self.NAHM = self.NAHM - (min_mem_limit - mem_limit)
						mem_limit = min_mem_limit

					mp.Process(target = container.setMemLimit2, args=(mem_limit,)).start()
					log_vmc.info('Provider T2: %s, new CML T2\u25BC: %d, new NAHM\u25B2: %d', container.name, mem_limit, self.NAHM)

			elif (container.getMemoryState() == 'STABLE') and (container.getMemoryStateTime() > (container.last_repo + self.sched_interval * repo_interval)):
				log_vmc.info('Container %s Block Repo TRUE -> FALSE', container.name)
				container.block_repo = False

			index += 1


	def increase_container_memory_limits(self, host: Host): # Algoritmo 7
		log_vmc.info('NAHM: %d', self.NAHM)
		log_vmc.info('Pause Demand: %d, Urgent Demand: %d, Needed Demand: %d', self.pause_demand, self.memory_urgent, self.memory_needed)

		if(self.NAHM > 0) and (self.pause_demand > 0):
			sorted_list = sorted(host.container_active_list, key=lambda container: container.getRunningTime(), reverse=True)
			index = 0

			while(self.NAHM > 0) and (index < len(sorted_list)):
					container = sorted_list[index]
					delta = container.getDeltaMemory()

					if(container.getContainerState() == 'PAUSED') and (delta <= self.NAHM):
						self.NAHM -= delta
						mem_limit = container.getMemoryLimitPG() + delta

						container.unpauseContainer()
						container.setMemLimit2(mem_limit)
						log_vmc.info('Unpaused: %s, Delta: %d, new CML T3\u25B2: %d, new NAHM\u25BC: %d', container.name, delta,
									container.getMemoryLimitPG(), self.NAHM)
						container.mem_delta_check = True
						self.pause_demand -= delta
						self.pause_count -= 1

					index += 1

		if(self.NAHM > 0) and (self.memory_urgent > 0):
				sorted_list = sorted(self.urgent_list, key=lambda container: container.getRunningTime(), reverse=True)
				index = 0

				while(self.NAHM > 0) and (index < len(sorted_list)):
					container = sorted_list[index]
					delta = container.getDeltaMemory()

					if self.NAHM >= delta:
						self.NAHM -= delta
						mem_limit = container.getMemoryLimitPG() + delta

						container.setMemLimit2(mem_limit)
						log_vmc.info('Urgent: %s, Delta: %d, new CML T3\u25B2: %d, new NAHM\u25BC: %d', container.name, delta,
									container.getMemoryLimitPG(), self.NAHM)
						container.mem_delta_check = True
						self.memory_urgent -= delta
						self.urgent_list.remove(container)

					index += 1

		if(self.NAHM > 0) and (self.memory_needed > 0):
				sorted_list = sorted(self.need_list, key=lambda container: container.getRunningTime(), reverse=True)
				index = 0

				while(self.NAHM > 0) and (index < len(sorted_list)):
					container = sorted_list[index]
					delta = container.getDeltaMemory()

					if (self.memory_needed > self.NAHM) and (container.mem_repo == True) and (container.block_repo == False):
						self.memory_needed -= delta
						self.need_list.remove(container)

					elif self.NAHM >= delta:
						self.NAHM -= delta
						mem_limit = container.getMemoryLimitPG() + delta

						container.setMemLimit2(mem_limit)
						log_vmc.info('Needed: %s, Delta: %d, new CML T3\u25B2: %d, new NAHM\u25BC: %d', container.name, delta,
									container.getMemoryLimitPG(), self.NAHM)
						container.mem_delta_check = True
						self.memory_needed -= delta
						self.need_list.remove(container)

					index += 1


	def container_suspension(self, host: Host): # Algorithm 9
		susp_thd = 786432 # 3GB in pages
		total_demand = self.memory_needed + self.memory_urgent + self.pause_demand
		log_vmc.info('Total Demand Unattended: %d', total_demand)
		mem_available = self.NAHM
		numCount = len(self.urgent_list) + len(self.need_list)
		candidates = numCount + self.pause_count - 1

		#sorted_list = sorted(host.container_active_list, key=lambda container: container.getRunningTime())
		sorted_list = sorted(host.container_active_list, key=lambda container: container.getMemoryLimitPG(), reverse=True)
		index = 0
		hold = False
		C1 = None
		C2 = None

		log_vmc.info('---------------------------------------------------------')
		log_vmc.info('Phase1 - Select Suspension Candidate:')

		while(candidates > 0) and index < len(host.container_active_list):
			C1 = sorted_list[index]
			log_vmc.info('Candidate: %s, CML: %d, CMU: %d', C1.name, C1.getMemoryLimitPG(), C1.getUsedMemoryPG())

			if (C1 in self.urgent_list) or (C1 in self.need_list) or (C1.getContainerState() == 'PAUSED'):

				if hold == True:

					if C1.getMemoryLimitPG() >= (total_demand - C1.getDeltaMemory()):

						if (C1.getUsedMemoryPG() < C2.getUsedMemoryPG()) or (C2.getMemoryLimitPG() < total_demand - C2.getDeltaMemory()):
							C2 = C1
							log_vmc.info('Chosen: %s', C2.name)

					candidates -= 1

				else:
					C2 = C1
					hold = True
					log_vmc.info('Chosen: %s', C2.name)

			index += 1

		if hold == True:

			if C2 in self.urgent_list:
				self.memory_urgent -= C2.getDeltaMemory()
				self.urgent_list.remove(C2)
				numCount -= 1

			elif C2 in self.need_list:
				self.memory_needed -= C2.getDeltaMemory()
				self.need_list.remove(C2)
				numCount -= 1

			elif C2.getContainerState() == 'PAUSED':
				self.pause_demand -= C2.getDeltaMemory()
				self.pause_count -= 1
				log_vmc.info('Unpausing Container: %s', C2.name)
				C2.unpauseContainer()

			# Suspend Container
			log_vmc.info('---------------------------------------------------------')
			log_vmc.info('Phase2 - Suspending Container: %s, CML: %d, CMU: %d, CSU: %d', C2.name, C2.getMemoryLimitPG(),
						C2.getUsedMemoryPG(), C2.getUsedSwapPG())

			C2.setContainerState('SUSPENDING')
			host.unlock_cores(C2.getCpuset())

			#mp.Process(target = C2.suspendContainer, daemon=True).start()
			#Thread(target = C2.suspendContainer).start()
			C2.suspendContainer()
			C2.inactive_time = datetime.now()
			host.container_inactive_list.append(C2)
			host.container_active_list.remove(C2)
			log_vmc.info('Container %s moved during Suspension from Active -> Inactive with status %s.', C2.name, C2.state)
			self.NAHM += C2.getMemoryLimitPG()
			log_vmc.info('new NAHM\u25B2: %d', self.NAHM)

			if C2.getUsedMemoryPG() <= susp_thd:
				log_vmc.info('---------------------------------------------------------')
				log_vmc.info('Phase3 - Increase Memory Limits with Recovered NAHM = %d', self.NAHM)
				self.increase_container_memory_limits(host)

		numCount = len(self.urgent_list) + len(self.need_list)

		if numCount > 0:
			index = 0
			log_vmc.info('---------------------------------------------------------')
			log_vmc.info('Phase4 - Pausing Unattended Containers:')

			while index < len(self.urgent_list):
				container = self.urgent_list[index]

				if (container.getDeltaMemory() / self.swapin_rate) > (self.sched_interval + self.latency):
					log_vmc.info('Pausing Container: %s', container.name)
					container.pauseContainer()

				index += 1


	def pause_suspend_running_containers(self, host:Host): # Algorithm 8
		runCount = len(host.container_active_list) - self.pause_count
		numCount = len(self.urgent_list) + len(self.need_list)
		log_vmc.info ("RunCount: %d, NumCount: %d", runCount, numCount)
		log_vmc.info('Remained NAHM: %d', self.NAHM)

		if(numCount + self.pause_count) > 1:
			self.container_suspension(host)

		elif(numCount == 1):

				if(runCount > 1) and (self.memory_urgent != 0):
					container = self.urgent_list[0]
					log_vmc.info('Pause Container: %s', container.name)
					container.pauseContainer()

				else:

					if self.memory_urgent == 0:
						container = self.need_list[0]

					else:
						container = self.urgent_list[0]

					mem_limit = container.getMemoryLimitPG() + self.NAHM
					container.setMemLimit2(mem_limit)
					self.NAHM = 0
					log_vmc.info('Receive Remained NAHM: %s, new CML T3\u25B2: %d, new NAHM\u25BC: %d', container.name,
								container.getMemoryLimitPG(), self.NAHM)

		elif (runCount == 0):
			container = host.container_active_list[0]
			mem_limit = container.getMemoryLimitPG() + self.NAHM
			log_vmc.info('Unpause Container: %s', container.name)
			container.unpauseContainer()
			container.setMemLimit2(mem_limit)
			self.NAHM = 0
			log_vmc.info('Receive Remained NAHM: %s, new CML T3\u25B2: %d, new NAHM\u25BC: %d', container.name,
						container.getMemoryLimitPG(), self.NAHM)


	def start_resume_inactive_container(self, host:Host): # Algoeirhm 10

		sorted_list = sorted(host.container_inactive_list, key=lambda container: container.getInactiveTime(), reverse=True)
		index = 0

		while(self.NAHM > 0) and (index < len(sorted_list)):
			container = sorted_list[index]

			if (container.getContainerState() == 'SUSPENDED'):
				new_limit = container.getMemoryLimitPG() + container.getDeltaMemory()

				if (new_limit <= self.NAHM) and (host.has_free_cores() >= container.request_cpus):
					cpu_allocation = host.get_available_cores(container.request_cpus)
					container.setContainerState('RESUMING')
					Thread(target = container.resumeContainer, args=(cpu_allocation, new_limit)).start()
					host.container_active_list.append(container)
					host.container_inactive_list.remove(container)
					log_vmc.info('Container %s moved during Resume from Inactive -> Active with status %s.', container.name, container.state)
					container.inactive_time = 0
					self.NAHM -= new_limit
					log_vmc.info('new NAHM\u25BC: %d', self.NAHM)

			index += 1

		sorted_list = sorted(host.container_inactive_list, key=lambda container: container.getInactiveTime(), reverse=True)
		index = 0

		while(self.NAHM > 0) and (index < len(sorted_list)):
			container = sorted_list[index]

			if (container.getContainerState() == 'QUEUED'):
				#mem_req = container.getMinMemoryLimitPG()
				mem_req = round(int(parser['Container']['initial_memory_limit']) / mmap.PAGESIZE)

				if (mem_req <= self.NAHM) and (host.has_free_cores() >= container.request_cpus):
					cpu_allocation = host.get_available_cores(container.request_cpus)

					if parser['Container']['type'] == 'LXC':
						container.startContainer()
						#container.setMemLimit(str(mem_req), str(swap))
						container.setMemLimit2(mem_req)
						container.setCPUCores(cpu_allocation)

					elif parser['Container']['type'] == 'DOCKER':
						mem_req = mem_req * mmap.PAGESIZE
						swap = mem_req + psutil.swap_memory().total
						container.startContainer(memory_limit=mem_req, swap_limit=swap, cpuset=cpu_allocation)

					host.container_active_list.append(container)
					host.container_inactive_list.remove(container)
					log_vmc.info('Container %s moved during Start from Inactive -> Active with status %s.', container.name, container.state)
					container.inactive_time = 0
					container.last_data_analyzed = container.start_time
					container.repo_lim = container.getMinMemoryLimitPG()
					self.NAHM -= container.getMemoryLimitPG()
					log_vmc.info('new NAHM\u25BC: %d', self.NAHM)

			index += 1
