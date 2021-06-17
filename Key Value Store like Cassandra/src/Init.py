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

import math
import socket

##
keyRangeStart = 0
keyRangeEnd = 255

def validateIpAndPort(ip, port):
	ipOctArr = ip.split(".")
	if len(ipOctArr) != 4:
		raise Exception(f"Ip '{ip}' is invalid")
	if not all(int(octet)>=0 and int(octet)<=255 for octet in ipOctArr):
		raise Exception(f"Ip '{ip}' is invalid")
	#
	if not (port>=1 and port <=65535):
		raise Exception(f"Port '{port}' is invalid")



def getRepInfoMap(replicasFilename):
	lineCount = 0
	ipPortSet = set()
	repInfoList = []
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
		repInfoList.append(ReplicaInfo(lineCount, ip, port))
		lineCount+=1
	#
	# print("repInfoList = ")
	# print(*repInfoList)
	totalKeys = keyRangeEnd - keyRangeStart + 1
	assert totalKeys > 0
	noOfKeysInEachNode = math.ceil(totalKeys / lineCount)
	#
	repInfoMap = {}
	curStart = keyRangeStart
	for i in range(len(repInfoList)):
		repInfo = repInfoList[i]
		curEnd = curStart+noOfKeysInEachNode - 1
		if(curEnd > keyRangeEnd):
			assert i == len(repInfoList) - 1
			curEnd = keyRangeEnd
		repInfo.startingKey = curStart
		repInfo.endingKey = curEnd
		curStart = curEnd + 1
		repInfoMap[repInfo.index] = repInfo
	#
	print(*repInfoMap.items(), sep = "\n")
	return repInfoMap

def initialiseNodes(replicasFilename, isFirstTime):
	repInfoMap = getRepInfoMap(replicasFilename)
	for (servIndex, repInfo) in repInfoMap.items():
		#print(f" repInfoMap {str(repInfo)}")
		transport = TTransport.TBufferedTransport(TSocket.TSocket(repInfo.ip, repInfo.port))
		client = KeyValueStore.Client(TBinaryProtocol.TBinaryProtocol(transport))
		try:
			transport.open()
			#print("opened ")
			client.initialiseNodes(repInfoMap, isFirstTime)
			#print("Init called")
			transport.close()
			#print("closed")
		except TTransport.TTransportException as te:
			#print("server down")
			transport.close()
			# Servers can be down if not the first time
			if isFirstTime:
				raise Exception("A server is down the first time")
		except Exception as e:
			# Should not happen
			transport.close()
			raise e











if __name__ == "__main__":
	if sys.version_info[0] < 3:
		raise Exception("Must be using Python 3+")
	if len(sys.argv) != 3:
		raise Exception("Run command should be of the form 'python3.x src/Init.py <replicaFilePath> <isFirstTime(T/F)>")
	initialiseNodes(sys.argv[1], sys.argv[2] == "T")