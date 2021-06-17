#!/usr/bin/env python3

import glob
import sys
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/cs557-inst/thrift-0.13.0/lib/py/build/lib*')[0])

from store import KeyValueStore
from store.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import numpy


def validateIpAndPort(ip, port):
	ipOctArr = ip.split(".")
	if len(ipOctArr) != 4:
		raise Exception(f"Ip '{ip}' is invalid")
	if not all(int(octet)>=0 and int(octet)<=255 for octet in ipOctArr):
		raise Exception(f"Ip '{ip}' is invalid")
	#
	if not (port>=1 and port <=65535):
		raise Exception(f"Port '{port}' is invalid")

def getServerList(replicasFilename):
	lineCount = 0
	ipPortSet = set()
	repInfoMap = {}
	replicasFileLines = open(replicasFilename, mode='r').read().strip().splitlines()
	for line in replicasFileLines:
		line = line.strip()
		if(not line):
			continue
		lineArr = line.split(":")
		if len(lineArr) != 2:
			raise Exception(f"Line '{line}' is not of the form <ip>:<port>")
		ip = lineArr[0]
		port = int(lineArr[1])
		validateIpAndPort(ip, port)
		ipPortCombo = ip+":"+str(port)
		if ipPortCombo in ipPortSet:
			raise Exception(f"The line {ipPortCombo} has more than 1 value")
		ipPortSet.add(ipPortCombo)
		repInfoMap[lineCount] = (ip, port)
		lineCount+=1
	return repInfoMap


def execute(replicasFilename):
	opers = ["1-put", "2-get", "3-changeCoord", "Anything else-exit"];
	repInfoMap = getServerList(replicasFilename)
	print("Servers = ")
	print(*repInfoMap.items(), sep = "\n")
	servIndex = int(input("\nChoose server(enter index) : "))
	ip = repInfoMap[servIndex][0]
	port = repInfoMap[servIndex][1]
	
	while True:
		try:
		
			transport = TTransport.TBufferedTransport(TSocket.TSocket(ip, port))
			client = KeyValueStore.Client(TBinaryProtocol.TBinaryProtocol(transport))
			
			operInt = int( input(f"Enter your choice \n {str(opers)} \n") )
			if(operInt == 1):
				key = int(input("Enter Key : "))
				value = input("Enter Value : ")
				consNum = int(input("Enter Consistency(1-One, 2-Quorum) : "))
				cLevel = Consistency.ONE if consNum == 1 else Consistency.QUORUM
				# print(f"cLevel = {str(cLevel)} + consNum = {str(consNum)}" )
				transport.open()
				client.writeInCoord(PairInfo(key, ValueAndTime(value, None)), cLevel)
				transport.close()
				print("Value written successfully")
				
			elif(operInt == 2):
				key = int(input("Enter Key : "))
				consNum = int(input("Enter Consistency(1-One, 2-Quorum) : "))
				cLevel = Consistency.ONE if consNum == 1 else Consistency.QUORUM
				# print(f"cLevel = {str(cLevel)} consNum = {str(consNum)} typecLevel={str(type(cLevel))}")
				# print(f"key = {str(key)} keytype = {str(type(key))}")
				transport.open()
				result = client.readFromCoord(key, cLevel)
				print(f"value = {result.valStr}; timestamp = {result.timeInMills}")
				transport.close()
				
			elif(operInt == 3):
				print("Servers = ")
				print(*repInfoMap.items(), sep = "\n")
				servIndex = int(input("\nChoose server(enter index) : "))
				ip = repInfoMap[servIndex][0]
				port = repInfoMap[servIndex][1]
				transport.close()
			else:
				transport.close()
				break
		except SystemException as se:
			# Wrong inputs
			transport.close()
			print("se = " + se.message)
		except Exception as e:
			#wrong code
			transport.close()
			print("gen e = " + str(e))
			template = "An exception of type {0} occurred. Arguments:\n{1!r}"
			message = template.format(type(e).__name__, e.args)
			print(message)

if __name__ == '__main__':
	try:
		if sys.version_info[0] < 3:
			raise Exception("Must be using Python 3+")
		if len(sys.argv) != 2:
			raise Exception("Run command should be of the form 'python3.x src/PythonClient.py <replicaFilePath>")
		execute(sys.argv[1])
	except Thrift.TException as tx:
		print('%s' % tx.message)
