'''
    Simple socket server using threads
'''

import socket
import sys
import os
import subprocess
import pickle
import time
import threading
import traceback
from DistributedRenderEv import *
from threading import Thread, Lock
import signal

is_py2 = sys.version[0] == '2'
if is_py2:
	import Queue as queue
else:
	import queue as queue

if is_py2:
	from thread import *
else:
	from _thread import *

shutdown = False
HOST = ''   # Symbolic name meaning all available interfaces
PORT = 8939 # Arbitrary non-privileged port

# The file queue to render
file_queue = queue.Queue()

# Synchronization queue used by the slaves and the distributor.
# When a slave is added or it finished work or it dies an event
# is placed on this queue.
render_file_work_queue = queue.Queue()

debugout = open("DistributedRenderer.dbg", 'w')
def dprint(*args, **kwargs):
	#print(*args, **kwargs)
	print(*args, file=debugout, **kwargs)

def signal_handler(signal, frame):
	print("Shutting down")
	debugout.close()
	shutdown = True
	exit()

signal.signal(signal.SIGINT, signal_handler)

class BlendFile(object):
	def __init__(self, name):
		self.name = name
		self.start_frame, self.end_frame = self.pull_blend_animation_info()
		dprint(self.start_frame, self.end_frame)
		self.pending_frame_q = queue.Queue()
		self.rendering_frame_list = []
		self.complete_frame_list = []
		self.list_mutex = Lock()
		i = self.start_frame
		while (i <= self.end_frame):
			f = BlendFrame(i, BlendFrame.RENDER_PENDING, self.name)
			self.pending_frame_q.put_nowait(f)
			i += 1

	def pull_blend_animation_info(self):
		dprint('grabbing info from:', self.name)
		rc = subprocess.call(["blender", self.name, "-b", "--python",
				      "blender_animation_frame_query.py"],
				      stdout=debugout, stderr=debugout)
		info_f = self.name + '.frame_info'
		f = open(info_f, 'r')
		start_frame = int(f.readline())
		end_frame = int(f.readline())
		f.close()
		os.remove(info_f)
		return start_frame, end_frame

	def get_start_frame(self):
		return self.start_frame

	def get_end_frame(self):
		return self.end_frame

	def get_name(self):
		return self.name

	def is_file_rendered(self):
		if self.pending_frame_q.qsize() > 0:
			return False
		else:
			return True

	def find_frame(self, frame_num, frame_list):
		for f in frame_list:
			if f.get_frame_num() == frame_num:
				return f
		return None

	def send_frame(self, slave):
		dprint("number of pending frames = ",  self.pending_frame_q.qsize())
		try:
			f = self.pending_frame_q.get_nowait()
		except queue.Empty:
			dprint(self.name, "finished rendering")
			return -1
		# set the state to active and place on the rendering list
		f.set_state(BlendFrame.RENDER_ACTIVE)
		self.list_mutex.acquire()
		self.rendering_frame_list.append(f)
		self.list_mutex.release()
		dprint("pickled the frame")
		pickled_frame = pickle.dumps(f)
		try:
			slave.send(pickled_frame)
		except:
			# couldn't send place it back on the pending queue
			self.pending_frame_q.put_nowait(f)
			return -2
		dprint("waiting to receive from slave")
		# now wait for confirmation that it has been rendered
		try:
			data = slave.recv(4096)
		except:
			self.pending_frame_q.put_nowait(f)
			dprint("couldn't receive from slave")
			return -2

		# now that we received the frame unpickle
		try:
			received_frame = pickle.loads(data)
		except:
			dprint("Unexpected error with pickle:\n", traceback.format_exc())
			self.pending_frame_q.put_nowait(f)
			return -2

		dprint("FRAME STATE from slave = ", received_frame.get_state())
		if received_frame.get_state() == BlendFrame.RENDER_SUCCESS:
			dprint("frame ", received_frame.get_frame_num(), "rendered successfully")
			self.list_mutex.acquire()
			f = self.find_frame(received_frame.get_frame_num(), self.rendering_frame_list)
			if f == None:
				dprint("Couldn't find frame", received_frame.get_frame_num(), "in file: ", self.name, ". continuing to render")
			self.complete_frame_list.append(f)
			self.rendering_frame_list.remove(f)
			self.list_mutex.release()
		return 0

class bcolors:
	GREY = '\033[1;30;40m'
	RED = '\033[1;31;40m'
	GREEN = '\033[1;32;40m'
	YELLOW = '\033[1;33;40m'
	BLUE = '\033[1;34;40m'
	MAGENTA = '\033[1;35;40m'
	CYAN = '\033[1;36;40m'
	WHITE = '\033[1;37;40m'
	HEADER = '\033[95m'
	OKBLUE = '\033[94m'
	OKGREEN = '\033[92m'
	WARNING = '\033[93m'
	FAIL = '\033[91m'
	ENDC = '\033[0m'
	BOLD = '\033[1m'
	UNDERLINE = '\033[4m'

class SlaveData(object):
	def __init__(self, slave_id, slave_thr=None):
		self.work_queue = queue.Queue()
		self.slave_id = slave_id
		self.slave_thr = slave_thr
		self.bfs_mutex = Lock()
		self.bfs = {}

	def get_id(self):
		return self.slave_id

	def get_slave_thread(self):
		return self.slave_thr

	def get_work_queue(self):
		return self.work_queue

	def set_slave_thread(self, thr):
		self.slave_thr = thr

	def add_bf(self, bf):
		self.bfs_mutex.acquire()
		if bf.get_name() in self.bfs:
			self.bfs[bf.get_name()][1] += 1
		else:
			self.bfs[bf.get_name()] = [bf, 1]
		self.bfs_mutex.release()

	def dump_stats(self):
		dprint("SlaveData: dumping stats")
		self.bfs_mutex.acquire()
		if bool(self.bfs):
			info = bcolors.RED + self.slave_id + bcolors.ENDC + '\n'
		for key in self.bfs:
			bf = self.bfs[key][0]
			total_frames = self.bfs[key][1]
			info += bcolors.BLUE + bf.get_name() + bcolors.ENDC + '\n'
			info += '\t' + bcolors.GREEN + 'total frames: ' + bcolors.ENDC + str(total_frames) + '\n\n'
			print(info)
			info = ''
		self.bfs_mutex.release()

class SlaveRenderer(object):
	def __init__(self):
		self.s = None
		# mutex for the slaves_dict
		self.slaves_dict_lock = Lock()
		# dictionary containing all active slaves
		self.slaves_dict = {}

	def __del__(self):
		self.s.close()

	def list_slaves(self):
		self.slaves_dict_lock.acquire()
		i = 0
		for key in self.slaves_dict:
			print(i, ". ", self.slaves_dict[key].get_id())
			i += 1
		self.slaves_dict_lock.release()

	def dump_stats(self):
		dprint("SlaveRenderer - dumping stats")
		self.slaves_dict_lock.acquire()
		for key in self.slaves_dict:
			self.slaves_dict[key].dump_stats()
		self.slaves_dict_lock.release()

	def setup_listener(self):
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		dprint('Socket created')
		#Bind socket to local host and port
		try:
			self.s.bind((HOST, PORT))
		except socket.error as msg:
			dprint('Bind failed.', msg)
			self.s.close()
			return -1

		dprint('Socket bind complete')

		#Start listening on socket
		self.s.listen(10)
		dprint('Socket now listening')

		return 0

	def slave_conn_listener(self):
		while (self.setup_listener() == -1):
			num_trys =+ 1
			if (num_trys > 10):
				exit()
			time.sleep(5)

		#now keep talking with the client
		while not shutdown:
			#wait to accept a connection - blocking call
			try:
				conn, addr = self.s.accept()
			except:
				dprint('something wrong with accepting a connection. Ignoring')
				continue

			dprint('Connected with ' + addr[0] + ':' + str(addr[1]))
			slave_id = addr[0] + ':' + str(addr[1])

			self.slaves_dict_lock.acquire()
			# create a thread to monitor that slave
			slave_data = SlaveData(slave_id)
			slave = Thread(target=self.slave_handler, args=(conn, slave_data,))
			slave_data.set_slave_thread(slave)
			slave.start()
			# store the slave in our DB
			dprint("slave ident = ", slave.ident)
			self.slaves_dict[slave.ident] = slave_data
			self.slaves_dict_lock.release()

			# create an even to let the distributor know about
			# the new slave.
			event = SlaveEvent(SlaveEvent.EVENT_SLAVE_ADDED, [slave.ident])
			dprint("slave added", slave_id)
			render_file_work_queue.put_nowait(event)

		dprint('shutting down connection listener')
		self.s.close()
		self.slaves_dict_lock.acquire()
		copy_dict = self.slaves_dict
		self.slaves_dict_lock.release()
		for key in copy_dict:
			slave = copy_dict[key].get_slave_thread()
			slave.join()

	#Function for handling connections. This will be used to create threads
	def slave_handler(self, conn, slave_data):
		dead = False
		wrq_queue = slave_data.get_work_queue()
		slave_id = slave_data.get_id()
		dprint("Starting slave handler for", slave_id)
		#infinite loop so that function do not terminate and thread do not end.
		while not shutdown or not dead:
			rc = 0
			# wait on the work queue for requests
			blendFile = wrq_queue.get()
			dprint("got a blend file")
			while (rc != -1):
				#try:
				rc = blendFile.send_frame(conn)
				#except:
				#	dead = True
				#	break;
				dprint("send_frame returned = ", rc)
				if (rc != 0 and rc != -1):
					dprint("Slave", slave_id, "had a problem. Stopped using it.")
					conn.close()
					dead = True
					break
				# slave actually processed the frame
				# account for it
				if rc == 0:
					slave_data.add_bf(blendFile)
			if (dead):
				event = SlaveEvent(SlaveEvent.EVENT_SLAVE_DEAD,
						[threading.get_ident()])
			else:
				event = SlaveEvent(SlaveEvent.EVENT_SLAVE_FINISHED_WORK,
						[threading.get_ident()])
			dprint("finished one send_frame", dead)
			render_file_work_queue.put_nowait(event)
		dprint('shutting down slave handler')


	"""
	distributor() is called with the file list.
	The way the system works is that whenever a slave is connected an
	event is added to the render_file_work_queue.
	This will trigger the function to wake up as long as there are files
	to render and it'll start rendering them in order
	"""
	def distributor(self):
		dprint('starting distributor thread')
		while not shutdown:
			for bf in iter(file_queue.get, None):
				while not bf.is_file_rendered():
					dprint("got a bf file")
					event = render_file_work_queue.get()
					dprint("got an event")
					event_type = event.get_type()
					if (event_type == event.EVENT_SLAVE_DEAD):
						dead_key_list = event.get_slave_keys()
						for key in dead_key_list:
							self.slaves_dict_lock.acquire()
							dprint('Removing ', key, ' from list of slave renderers');
							del(self.slaves_dict[key])
							self.slaves_dict_lock.release()
						continue

					slave_key_list = event.get_slave_keys()

					for key in slave_key_list:
						self.slaves_dict_lock.acquire()
						dprint('queuing work on: ', self.slaves_dict[key].get_id())
						slave_wq = self.slaves_dict[key].get_work_queue()
						self.slaves_dict_lock.release()
						slave_wq.put_nowait(bf)
		dprint('shutting down distributor thread')


def start_master(dRenderer):
	listener_thr = Thread(target=dRenderer.slave_conn_listener)
	distributor_thr = Thread(target=dRenderer.distributor)
	listener_thr.start()
	distributor_thr.start()
	return dRenderer, listener_thr, distributor_thr


if __name__ == '__main__':
	# first argument should be a file with a list of blend files to
	# render
	#
	#
#	args = sys.argv[1:]
#	try:
#		filename = args[0]
#	except:
#		dprint('Usage: DistributedRenderBE <file_name>')
#		exit()

	dRenderer = SlaveRenderer()

	try:
		dRen, lthr, dthr = start_master(dRenderer)
	except:
		dprint("Unexpected error:\n", traceback.format_exc())
		exit()

	msg = "Enter one of the following commands (exact):\n"+ \
	      "\tSPECIFY\t\t- specify a file which lists .blends to render\n"+ \
	      "\tSTATS\t\t- dump statistics\n"+ \
	      "\tSLAVE STATS\t- dump slave statistics\n"+ \
	      "\tLIST\t\t- list active slaves\n"+ \
	      "\tEXIT\t\t- exit Distributed Renderer\n"+ \
	      ">> "

	global_file_list = []

	while not shutdown:
		if is_py2:
			cmd = raw_input(msg)
		else:
			cmd = input(msg)
		if (not cmd):
			continue

		if cmd == 'SPECIFY':
			if is_py2:
				filename = raw_input("Please enter a file name: ")
			else:
				filename = input("Please enter a file name: ")
			if (not filename):
				continue

			try:
				f = open(filename, 'r')
			except:
				print('file: ', filename, 'could not be found')
				continue

			for l in f:
				bf_name = l.rstrip()
				try:
					bf = BlendFile(bf_name)
				except:
					dprint('Blender file: ', l, 'could not be opened')
					continue
				global_file_list.append(bf)
				file_queue.put_nowait(bf)
		elif cmd == 'SLAVE STATS':
			dRenderer.dump_stats()
		elif cmd == 'STATS':
			for bf in global_file_list:
				start_frame = bf.get_start_frame()
				end_frame = bf.get_end_frame()
				frames_rendered = len(bf.complete_frame_list)
				frames_in_progress = len(bf.rendering_frame_list)
				frames_pending = bf.pending_frame_q.qsize()
				info = bf.get_name() + ':\n' + \
				bcolors.GREEN + 'start frame = ' + bcolors.ENDC + str(start_frame) + '\n' + \
				bcolors.GREEN + 'end frame = ' + bcolors.ENDC + str(end_frame) + '\n' + \
				bcolors.GREEN + 'frames rendered = ' + bcolors.ENDC + str(frames_rendered) + '\n' + \
				bcolors.GREEN + 'frames in progress = ' + bcolors.ENDC + str(frames_in_progress) + '\n' + \
				bcolors.GREEN + 'frames pending = ' + bcolors.ENDC + str(frames_pending) + '\n\n'
				print(info)
		elif cmd == 'LIST':
			dRenderer.list_slaves()
		elif cmd == 'EXIT':
			# This doesn't actually work correctly because the
			# queues block. So even if shutdown is set to True
			# we won't be able to wake up until we get
			# something on the queue.
			# We need to have a timeout while waiting on the
			# queues.
			shutdown = True
			break;
		else:
			print("Unknown command")
			continue

	lthr.join()
	dthr.join()
	debugout.close()
