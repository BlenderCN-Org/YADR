class BlendFrame(object):
	RENDER_SUCCESS = 0
	RENDER_FAILED = 1
	RENDER_PENDING = 2
	RENDER_ACTIVE = 3

	def __init__(self, frame_num, state, file_name):
		self.file_name = file_name
		self.frame_num = frame_num
		self.state = state

	def completed_rendering(self, state):
		self.state = state

	def get_state(self):
		return self.state

	def set_state(self, state):
		self.state = state

	def get_file_name(self):
		return self.file_name

	def get_frame_num(self):
		return self.frame_num

class SlaveEvent(object):
	EVENT_SLAVE_ADDED = 0
	EVENT_SLAVE_DEAD = 1
	EVENT_SLAVE_FINISHED_WORK = 2

	def __init__(self, event_type, slave_key_list):
		self.event_type = event_type
		self.slave_key_list = slave_key_list

	def get_slave_keys(self):
		return self.slave_key_list

	def get_type(self):
		return self.event_type
