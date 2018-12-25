import socket
import sys
import os
import pickle
from DistributedRenderEv import *
import subprocess
import select
import errno
import time

class Slave(object):
	def __init__(self, ip, port):
		self.master_ip = ip
		self.master_port = int(port)
		self.recieved_frame = None
		self.server_address = (self.master_ip, self.master_port)
		self.inputs = []
		self.output = []
		self.garbage_recv = 0

	def reconnect(self):
		self.sock.close()
		self.sock.connect(self.server_address)
		self.sock.setblocking(0)
		self.inputs = [self.sock]
		self.outputs = []

	def connect_to_master(self):
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		connected = False
		while not connected:
			try:
				self.sock.connect(self.server_address)
			except:
				print("Couldn't connect to master. waiting 5 seconds before retrying")
				time.sleep(5)
				continue
			connected = True
		self.sock.setblocking(0)
		self.inputs = [self.sock]
		self.outputs = []
		i = 0
		while self.inputs:
			readable, writable, exceptional = select.select(
				self.inputs, self.outputs, self.inputs)
			#TODO not sure why it's not detecting
			#connection closure
			if self.garbage_recv > 100:
				exit(-1)
			for s in readable:
				if s is self.sock:
					self.read_from_master()
					self.render_frame()
			for s in exceptional:
				print("removing s")
				self.inputs.remove(s)

	def read_from_master(self):
		try:
			data = self.sock.recv(4096)
		except:
			print("problem reading from socket")
			self.reconnect()
			return
		print(data)
		try:
			self.recieved_frame = pickle.loads(data)
		except:
			print("received some garbage")
			self.garbage_recv = self.garbage_recv + 1
			self.recieved_frame = None
			return
		print(self.recieved_frame.get_file_name())
		print(self.recieved_frame.get_frame_num())

	def render_frame(self):
		if self.recieved_frame == None:
			return
		blendPath = self.recieved_frame.get_file_name()
		pathname = os.path.dirname(blendPath)
		frame_names = os.path.splitext(os.path.basename(blendPath))[0] + "-####"
		outputdir = pathname+'/'+'FinalOutput'
		subdirname = outputdir+'/'+os.path.splitext(os.path.basename(blendPath))[0]
		try:
			os.makedirs(outputdir)
		except OSError as e:
			if e.errno != errno.EEXIST and e.errno != errno.EPERM:
				print("Problem creating: ", outputdir, "errno = ", e.errno)
				return
		try:
			os.makedirs(subdirname)
		except OSError as e:
			if e.errno != errno.EEXIST:
				print("Problem creating: ", outputdir, "errno = ", e.errno)
				return

		framePath = subdirname+'/'+frame_names
		rc = subprocess.call(["blender", "-b", self.recieved_frame.get_file_name(),
				      "-o", framePath,
				      "-f", str(self.recieved_frame.get_frame_num())])
		if not os.path.isfile(framePath):
			self.recieved_frame.completed_rendering(BlendFrame.RENDER_FAILED)
		else:
			self.recieved_frame.completed_rendering(BlendFrame.RENDER_SUCCESS)

		pickled_frame = pickle.dumps(self.recieved_frame)
		try:
			self.sock.send(pickled_frame)
		except:
			print("problem sending to socket")
			self.reconnect()

if __name__ == "__main__":
	args = sys.argv[1:]
	if (len(args) < 2):
		print("usage: slave <ip> <port>")
		exit(1)
	slave = Slave(args[0], args[1])
	slave.connect_to_master()
