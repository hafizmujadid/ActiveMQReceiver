package org.apache.spark.streaming.activemq
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.activemq.ActiveMQConnectionFactory
import javax.jms.Connection
import javax.jms.DeliveryMode
import javax.jms.Destination
import javax.jms.ExceptionListener
import javax.jms.JMSException
import javax.jms.Message
import javax.jms.MessageConsumer
import javax.jms.Session
import javax.jms.TextMessage
import javax.jms.MessageListener
class ActiveMQReceiver(broker: String, queue: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Serializable {
	var receiverThread: Thread = null

	override def onStart() {
		receiverThread = new Thread(new Consumer(broker,queue))
		receiverThread.start()
	}
	override def onStop() {
		receiverThread.stop()
	}
	class Consumer(broker: String,queue:String) extends Runnable {
		val connectionFactory = new ActiveMQConnectionFactory(broker)
		val connection = connectionFactory.createConnection()
		val session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
		connection.start()
		val destination = session.createQueue(queue)
		val consumer = session.createConsumer(destination)
		override def run() {
			receive
		}
		def receive() = {
			try {
				while (true) {
					val message = consumer.receive(1000)
					if (message.isInstanceOf[TextMessage]) {
						val textMessage = message.asInstanceOf[TextMessage]
						val text = textMessage.getText
						store(text)
					} else {
						//store(message.toString())
					}
				}
			} catch {
				case e: Exception => {
					println("Caught: " + e)
					e.printStackTrace()
				}
			}
		}
	}
}
