# ActiveMQReceiver
Custom receiver for Spark streaming from active MQ source. 
#Spark version
It is valid for all spark versions. Just change spark-core and spark streaming version in pom file.
#Example usage
	
	```
	val streams = ActiveMQStreamUtil.createStream(broker, queue, ssc)
	streams.print
	ssc.start
	ssc.awaitTermination
	```
