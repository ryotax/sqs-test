import java.util.{Collections, List => JList, Map => JMap}

import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient
import com.amazonaws.services.sqs.model.{SendMessageRequest, DeleteMessageRequest, Message, ReceiveMessageRequest, ReceiveMessageResult}

import scala.collection.convert.decorateAsScala._

object Main {
  val SqsUrl = System.getenv("SQS_URL")

  def main(args: Array[String]): Unit = {
    println("start")
    val messages = args(0).toInt
    (new Thread {override def run(): Unit = {receiveMessage()}}).start()
    Thread.sleep(3000)
    sendMessages(messages)
  }

  def receiveMessage(): Unit = {
    val sqsAsync  = new AmazonSQSAsyncClient()
    val bufferedSqs = new AmazonSQSBufferedAsyncClient(sqsAsync)

    val receiveRq = new ReceiveMessageRequest()
            .withMaxNumberOfMessages(1)
            .withQueueUrl(SqsUrl)

    var message = 0
    var noMessage = 0
    while (noMessage < 3) {
      val rx: ReceiveMessageResult = bufferedSqs.receiveMessage(receiveRq)
      val messageBuffer = rx.getMessages.asScala
      if (messageBuffer.isEmpty) {
        noMessage += 1
      } else {
        for (msg <- messageBuffer) {
          consumeMessage(msg)
          message += 1
          println(s"n = ${message}")
          bufferedSqs.deleteMessageAsync(new DeleteMessageRequest(SqsUrl, msg.getReceiptHandle))
        }
      }
      println("loop")
    }
    println("received")
    System.exit(0);
  }

  private def consumeMessage(msg: Message) {
    println(s"process ${msg.getBody}, ${msg.getMessageId}")
    Thread.sleep(10)
  }

  def sendMessages(n: Int): Unit = {
    println("send messages")
    val sqsAsync  = new AmazonSQSAsyncClient()
    (0 until n) foreach {i =>
      val req = new SendMessageRequest(SqsUrl, s"msg: ${i}")
      sqsAsync.sendMessageAsync(req)
    }
    println("sent")
  }
}
