/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
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
