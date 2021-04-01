import random
import socket
import zmq

from anna.client import AnnaTcpClient
from anna.metadata_pb2 import (
	Tier, MEMORY
)

class AnnaRouterClient(AnnaTcpClient):
	def __init__(self, elb_addr='127.0.0.1', ip='127.0.0.1', local=True, offset=0):
		super().__init__(elb_addr, ip, local, offset)

		# Port on which routers listen for node join/remove messages
		self.notify_port = 6400

	def lookup(self, key, cache=False):
		# Performs a lookup of where KEY is stored by contacting an Anna router
		
		if cache:
			return self._get_worker_address(key)
		return self._query_routing(key, random.choice(self.elb_ports))

	def add_node(self, public_ip, private_ip, virtual_id):
		# Adds a node into Anna's routing hash ring
		
		msg = self.createNotifyMsg('join', public_ip, private_ip, virtual_id)
		self.sendMsg(msg)


	def remove_node(self, public_ip, private_ip, virtual_id):
		# Removes a node from Anna's routing hash ring
		
		msg = self.createNotifyMsg('depart', public_ip, private_ip, virtual_id)
		self.sendMsg(msg)

	def replace_node(self, public_ip, private_ip, virtual_id):
		msg = self.createNotifyMsg('replace', public_ip, private_ip, virtual_id)
		self.sendMsg(msg)

	def createNotifyMsg(self, event, public_ip, private_ip, virtual_id):
		tier = Tier.Name(MEMORY)
		msg = event + ':' + tier + ':' + public_ip + ':' + private_ip + ':0:' + virtual_id
		return msg

	def sendMsg(self, msg):
		dst_addr = 'tcp://' + self.elb_addr + ':' + str(self.notify_port)
		send_sock = self.pusher_cache.get(dst_addr)
		send_sock.send_string(msg)
