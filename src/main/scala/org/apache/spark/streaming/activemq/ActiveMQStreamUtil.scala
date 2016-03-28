package org.apache.spark.streaming.activemq
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
object ActiveMQStreamUtil {
	def createStream(broker: String, queue: String,
		ssc: StreamingContext): ReceiverInputDStream[String] = {
		ssc.receiverStream(new ActiveMQReceiver(broker, queue))
	}
}