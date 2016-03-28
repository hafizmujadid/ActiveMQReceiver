package org.apache.spark.streaming.activemq.test
import org.scalatest.Assertions
import org.scalatest.junit.JUnitSuite
import org.junit.Before
import org.junit.Test
import org.junit._
import Assert._
import org.junit.After
import org.scalatest.BeforeAndAfterAll
import org.apache.activemq.ActiveMQConnectionFactory
import javax.jms.Connection
import javax.jms.DeliveryMode
import javax.jms.Destination
import javax.jms.ExceptionListener
import javax.jms.JMSException
import javax.jms.Message
import javax.jms.MessageConsumer
import javax.jms.MessageProducer
import javax.jms.Session
import javax.jms.TextMessage
import scala.util.Random
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.activemq.ActiveMQStreamUtil
@Test
class AppTest extends JUnitSuite with BeforeAndAfterAll {
	val host = "localhost"
	val port = "61616"
	val queue = "test"
	@Before def ininitialize() {
		send
	}

	@Test def streamTest() {
		val broker = s"tcp://$host:$port"
		val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))
		val ssc = new StreamingContext(sc, Duration(30 * 1000))
		val streams = ActiveMQStreamUtil.createStream(broker, queue, ssc)
		streams.print
		ssc.start
		ssc.awaitTermination(100000)
	}
	@After def CleanUp() {

	}
	def send() = {
		val broker = s"tcp://$host:$port"
		try {
			val connectionFactory = new ActiveMQConnectionFactory(broker)
			val connection = connectionFactory.createConnection()
			connection.start()
			val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
			val destination = session.createQueue(queue)
			val producer = session.createProducer(destination)
			producer.setDeliveryMode(DeliveryMode.PERSISTENT)
			while (true) {
				for (i <- 0 until 100) {
					val message = session.createTextMessage("This is " + i + "th message")
					Thread.sleep(1000)
					producer.send(message)
				}
			}
			session.close()
			connection.close()
		} catch {
			case e: Exception => {
				println("Caught: " + e)
				e.printStackTrace()
			}
		}

	}
}