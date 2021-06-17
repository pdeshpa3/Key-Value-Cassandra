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

import time
import os
import numpy




keyRangeStart = 0
keyRangeEnd = 255
replicationFactor = 3



class ConnectionToReplica:
	def __init__(self, ip, port):
		self.transport = TTransport.TBufferedTransport(TSocket.TSocket(ip, int(port)))
		self.client = KeyValueStore.Client(TBinaryProtocol.TBinaryProtocol(self.transport))


class StoreHandler:
	def __init__(self, ip, port):
		self.isServerJustUp = True
		self.curReplInfo = ReplicaInfo(-1, ip, port);
		self.replicaMap = {} #{servIndex : ReplicaInfo}
		self.hintsMap = {}  #{servIndex : {Map<key, ValueAndTime>}}
		self.kvstore = {}  #Map<key, ValueAndTime> store

		
	def getStoreContentFromPersistentStorage(self):
		if not os.path.exists(f"logFile_{self.curReplInfo.index}.txt"):
			return
		#
		fileLines = open(f"logFile_{self.curReplInfo.index}.txt", "r").read().strip().splitlines()
		for line in fileLines:
			line = line.strip()
			if not line:
				continue
			lineSplt = line.split(":")
			assert len(lineSplt) == 3
			key = int(lineSplt[0])
			valStr = lineSplt[1]
			timeInMills = int(lineSplt[2])
			self.kvstore[key] = ValueAndTime(valStr, timeInMills)

	def initialiseNodes(self, repInfoMap, isFirstTime):
		#print("Hello world")
		self.replicaMap = repInfoMap
		for repInfo in repInfoMap.values():
			if self.curReplInfo.ip == repInfo.ip and self.curReplInfo.port == repInfo.port:
				self.curReplInfo = repInfo
				break
		#
		if isFirstTime: # First time doing the demo, no need for hinted handoff
			print("server details(first time) = " + str(self.curReplInfo))
			self.isServerJustUp = False
			if os.path.exists(f"logFile_{self.curReplInfo.index}.txt"):
				# For each new demo, we delete old log
				os.remove(f"logFile_{self.curReplInfo.index}.txt")
		#
		if self.isServerJustUp:
			print("server details(after restart) = " + str(self.curReplInfo))
			#
			# Replay log file first
			self.getStoreContentFromPersistentStorage()
			# Hinted handoff next
			for repInfo in repInfoMap.values():
				if self.curReplInfo.ip == repInfo.ip and self.curReplInfo.port == repInfo.port:
					continue # No need rpc to self for hinted handoff!
				#
				#print(f"repInfo ip = {str(repInfo.ip)} repInfo port = {str(repInfo.port)}")
				conToRep = ConnectionToReplica(repInfo.ip, repInfo.port)
				try:
					conToRep.transport.open()
					hints = conToRep.client.getHints(self.curReplInfo.index)
					conToRep.transport.close()
					self.flushHints(hints)
				except TTransport.TTransportException as te:
					# Another server can be down
					print("get hints in te")
					conToRep.transport.close()
				except Exception as e:
					# Should not happen
					conToRep.transport.close()
					raise e
		self.isServerJustUp = False

	#
	#

	def getHints(self, servIndex):
		return self.hintsMap.pop(servIndex, {})

	def flushHints(self, hints):
		if hints is None or len(hints) == 0:
			return
		#
		logFile = open(f"logFile_{self.curReplInfo.index}.txt", "a")
		for key, valAndTime in hints.items():
			if key in self.kvstore and self.kvstore[key].timeInMills > valAndTime.timeInMills:
				continue
			else:
				# Write the hint iff the hints's timestamp is greater than the 
				# existing timestamp of the key in the server(if present)
				logFile.write(str(key) + ":" + valAndTime.valStr + ":" + str(valAndTime.timeInMills))
				logFile.write("\n")
				self.kvstore[key] = valAndTime
		logFile.close()

	def updateHints(self, downServerIndices, pairInfo):
		for downServIndex in downServerIndices:
			if self.hintsMap.get(downServIndex) is None:
				self.hintsMap[downServIndex] = {}
			self.hintsMap[downServIndex][pairInfo.key] = pairInfo.valAndTime


	def getReplicasToCall(self, key):
		matchingServerIndex = None
		for index, repInfo in self.replicaMap.items():
			if key >= repInfo.startingKey and key <= repInfo.endingKey:
				matchingServerIndex = index
				break
		#
		if matchingServerIndex is None:
			raise SystemException(f"Key should be between {keyRangeStart} and {keyRangeEnd}")
		#
		replicasToCall = []
		for i in range(replicationFactor):
			replicasToCall.append(self.replicaMap.get( (matchingServerIndex + i) % len(self.replicaMap)))
		return replicasToCall


	#
	#

	def writeInCoord(self, pairInfo, cLevel):
		replicasToCall = self.getReplicasToCall(pairInfo.key)
		#print(f"replicas to call = {str(replicasToCall)} cLevel = {str(cLevel)}\n")
		#
		openRepConList = []
		downServerIndices = [] # Not all down server indices, only among the ones where we intend to write the current value
		isCoordAReplica = False
		for repServer in replicasToCall:
			if repServer.ip == self.curReplInfo.ip and repServer.port == self.curReplInfo.port:
				isCoordAReplica = True
			else:
				conToRep = ConnectionToReplica(repServer.ip, repServer.port)
				try:
					conToRep.transport.open()
					openRepConList.append(conToRep)
					# Means replica server is up
				except (TTransport.TTransportException, Exception) as te:
					#Means server is down, and we should add the hint
					conToRep.transport.close()
					pairInfo.valAndTime.timeInMills = int(time.time() * 1000)
					downServerIndices.append(repServer.index)
		#
		#print(f"downServerIndices = {str(downServerIndices)}")
		if len(replicasToCall)-len(downServerIndices) >= cLevel:
			self.updateHints(downServerIndices, pairInfo)
			if isCoordAReplica:
				self.putKeyValuePair(pairInfo)
			#
			for conToRep in openRepConList:
				try:
					conToRep.client.putKeyValuePair(pairInfo)
					conToRep.transport.close()
				except Exception as e:
					#Shouldn't happen
					conToRep.transport.close()
					raise e

		else:
			for conToRep in openRepConList:
				conToRep.transport.close()
			raise SystemException(f"Consistency level of {str(cLevel)} cannot be met")

	def writePairInfoInLog(self, pairInfo):
		logFile = open(f"logFile_{self.curReplInfo.index}.txt", "a")
		logFile.write(str(pairInfo.key) + ":" + pairInfo.valAndTime.valStr + ":" + str(pairInfo.valAndTime.timeInMills))
		logFile.write("\n")
		logFile.close()

	def putKeyValuePair(self, pairInfo):
		pairInfo.valAndTime.timeInMills = int(time.time() * 1000)
		self.writePairInfoInLog(pairInfo)
		self.kvstore[pairInfo.key] = pairInfo.valAndTime


	def readFromCoord(self, key, cLevel):
		replicasToCall = self.getReplicasToCall(key)
		#
		openRepConList = []
		downServerIndices = [] # Not all down server indices, only among the ones where we intend to read the value of the key
		allValAndTimesRead = []
		for repServer in replicasToCall:
			if repServer.ip == self.curReplInfo.ip and repServer.port == self.curReplInfo.port:
				allValAndTimesRead.append(self.getValueOfKey(key))
			else:
				conToRep = ConnectionToReplica(repServer.ip, repServer.port)
				try:
					conToRep.transport.open()
					openRepConList.append(conToRep)
					# Means replica server is up
				except (TTransport.TTransportException, Exception) as te:
					#Means server is down, and we should add the hint
					conToRep.transport.close()
					downServerIndices.append(repServer.index)
		#
		if len(replicasToCall)-len(downServerIndices) >= cLevel:
			for conToRep in openRepConList:
				try:
					allValAndTimesRead.append(conToRep.client.getValueOfKey(key))
					conToRep.transport.close()
				except Exception as e:
					#Shouldn't happen
					conToRep.transport.close()
					raise e

		else:
			for conToRep in openRepConList:
				conToRep.transport.close()
			raise SystemException(f"Consistency level of {str(cLevel)} cannot be met")
		#
		# All up servers will have the latest value of the key only, but still we check
		latestValAndTime = None
		for valAndTime in allValAndTimesRead:
			if latestValAndTime is None:
				latestValAndTime = valAndTime
			else:
				latestValAndTime = valAndTime if valAndTime.timeInMills > latestValAndTime.timeInMills else latestValAndTime
		#
		#print(f"latestValAndTime = {str(latestValAndTime)}")
		return latestValAndTime


	def getValueOfKey(self, key):
		if key in self.kvstore:
			return self.kvstore.get(key)
		else:
			raise SystemException(f"Key {key} not present in map")
