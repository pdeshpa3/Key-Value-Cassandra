#!/usr/bin/env python3

import glob
import sys
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from store import KeyValueStore
from store.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from PythonStoreHandler import StoreHandler
import socket


class Server:


	def __init__(self):
		self.log = {}
	
	

if __name__ == '__main__':
	
	ip = socket.gethostbyname(socket.gethostname())
	port=int(sys.argv[1])


	handler = StoreHandler(ip, port)
	processor = KeyValueStore.Processor(handler)
	
	transport = TSocket.TServerSocket(ip, port)
	tfactory = TTransport.TBufferedTransportFactory()
	pfactory = TBinaryProtocol.TBinaryProtocolFactory()

	server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

	# You could do one of these for a multithreaded server
	# server = TServer.TThreadedServer(
	#     processor, transport, tfactory, pfactory)
	# server = TServer.TThreadPoolServer(
	#     processor, transport, tfactory, pfactory)
	
	print('Starting the server...')
	server.serve()
	print('done.')

