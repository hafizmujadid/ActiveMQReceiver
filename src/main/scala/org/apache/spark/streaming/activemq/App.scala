package org.apache.spark.streaming.activemq

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration

/**
 * @author ${user.name}
 */
object App {

	val host = "localhost"
	val port = "61616"
	val queue = "test"

	def main(args: Array[String]) {
		val broker = s"tcp://$host:$port"
		val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))
		val ssc = new StreamingContext(sc, Duration(30 * 1000))
		val streams = ActiveMQStreamUtil.createStream(broker, queue, ssc)
		streams.print
		ssc.start
		ssc.awaitTermination
	}

}
