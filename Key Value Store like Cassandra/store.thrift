exception SystemException
{
  1: optional string message
}

struct ValueAndTime
{
	1: string valStr;
	2: i64 timeInMills;
}

struct PairInfo
{
	1: required i32 key;
	2: ValueAndTime valAndTime;
}

enum Consistency{
	ONE = 1,
	QUORUM = 2 
}

struct ReplicaInfo
{
	1: required i32 index;
	2: required string ip;
	3: required i32 port;
	4: required i32 startingKey;
	5: required i32 endingKey;
}

service KeyValueStore
{
	# isFirstTime is set to true when we run the init program for the first 
	# time. When a server goes down and comes back up also we run the init program but with isFirstTime false
	void initialiseNodes(1: map<i32, ReplicaInfo> repInfoMap, 2: bool isFirstTime)
		throws (1: SystemException systemException),

	map<i32, ValueAndTime> getHints(1: i32 serverIndex)
		throws (1: SystemException systemException),

	void writeInCoord(1: PairInfo pairInfo, 2: Consistency cLevel)
		throws (1: SystemException systemException),

	void putKeyValuePair(1: PairInfo pairInfo)
		throws (1: SystemException systemException),
	#
	ValueAndTime readFromCoord(1: i32 key, 2: Consistency cLevel)
		throws (1: SystemException systemException),

	ValueAndTime getValueOfKey(1: i32 key)
		throws (1: SystemException systemException),
}
