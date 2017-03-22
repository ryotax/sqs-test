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
    (new Thread {override def run(): Unit = {receiveMessage(messages)}}).start()
    Thread.sleep(3000)
    sendMessages(messages)
  }

  def receiveMessage(n: Int): Unit = {
    val sqsAsync  = new AmazonSQSAsyncClient()
    val bufferedSqs = new AmazonSQSBufferedAsyncClient(sqsAsync)

    val receiveRq = new ReceiveMessageRequest()
            .withMaxNumberOfMessages(1)
            .withQueueUrl(SqsUrl)

    var receives = 0
    while (receives < n) {
      val rx: ReceiveMessageResult = bufferedSqs.receiveMessage(receiveRq)
      for (msg <- rx.getMessages.asScala) {
        consumeMessage(msg)
        receives += 1
        println(receives)
        bufferedSqs.deleteMessageAsync(new DeleteMessageRequest(SqsUrl, msg.getReceiptHandle))
      }
      println("receive loop")
    }
    println("received")
    System.exit(0);
  }

  private def consumeMessage(msg: Message) {
    println(s"process ${msg.getBody}, ${msg.getMessageId}")
    Thread.sleep(100)
  }

  def sendMessages(n: Int): Unit = {
    println("send messages")
    val sqsAsync  = new AmazonSQSAsyncClient()
    (0 to n) foreach {i =>
      val req = new SendMessageRequest(SqsUrl, s"msg: ${i}")
      sqsAsync.sendMessageAsync(req)
    }
    println("sent")
  }
}
