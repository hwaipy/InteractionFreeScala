package com.interactionfree

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.charset.Charset
import java.time.LocalDateTime
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadFactory}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.language.dynamics
import org.zeromq.{SocketType, ZContext, ZLoop, ZMsg}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Random
import com.interactionfree.Logging
import com.interactionfree.Logging.Level._

object IFDefinition {
  val PROTOCOL = "IF1"
  val DISTRIBUTING_MODE_BROKER = "Broker"
  val DISTRIBUTING_MODE_DIRECT = "Direct"
  val DISTRIBUTING_MODE_SERVICE = "Service"
  val HEARTBEAT_LIVETIME = 10 * 1000
}

object Message {
  private val MessageIDs = new AtomicLong(0)

  def parse(content: Array[Array[Byte]]) = {
    val protocol = new String(content(1), "UTF-8")
    if (!IFDefinition.PROTOCOL.contains(protocol)) throw new IFException(s"Prorocol not available: ${protocol}")
    val messageID = new String(content(2), "UTF-8")
    val isOutgoingMessage = content.size == 7
    val distributingMode = if (isOutgoingMessage) new String(content(3), "UTF-8") else "Received"
    val remoteAddress = if (isOutgoingMessage) content(4) else content(4)
    val serialization = new String(content(if (isOutgoingMessage) 5 else 4), "UTF-8")
    val invocation = Invocation.deserialize(content(if (isOutgoingMessage) 6 else 5), serialization)
    new Message(messageID, isOutgoingMessage, distributingMode, remoteAddress, serialization, invocation)
  }

  private def nextMesssageID = MessageIDs.getAndIncrement().toString

  def newBrokerMessage(invocation: Invocation, serialization: String = "Msgpack") = new Message(nextMesssageID, true, IFDefinition.DISTRIBUTING_MODE_BROKER, Array[Byte](), serialization, invocation)

  def newServiceMessage(serviceName: String, invocation: Invocation, serialization: String = "Msgpack") = new Message(nextMesssageID, true, IFDefinition.DISTRIBUTING_MODE_SERVICE, serviceName.getBytes("UTF-8"), serialization, invocation)

  def newDirectMessage(address: Array[Byte], invocation: Invocation, serialization: String = "Msgpack") = new Message(nextMesssageID, true, IFDefinition.DISTRIBUTING_MODE_DIRECT, address, serialization, invocation)

  def newFromBrokerMessage(messageID: String, fromAddress: Array[Byte], invocation: Invocation, serialization: String = "Msgpack") = new Message(messageID, false, IFDefinition.DISTRIBUTING_MODE_DIRECT, fromAddress, serialization, invocation)
}

class Message private (val messageID: String, val isOutgoingMessage: Boolean, val distributingMode: String, val remoteAddress: Array[Byte], val serialization: String, val invocation: Invocation) {
  val protocol = "IF1"
  val isBrokerMessage = isOutgoingMessage && distributingMode == IFDefinition.DISTRIBUTING_MODE_BROKER
  val isServiceMessage = isOutgoingMessage && distributingMode == IFDefinition.DISTRIBUTING_MODE_SERVICE
  val isDirectMessage = isOutgoingMessage && distributingMode == IFDefinition.DISTRIBUTING_MODE_DIRECT
  private val creationTime = System.currentTimeMillis()

  def expired(lifetime: Long) = System.currentTimeMillis() - creationTime > lifetime

  def messageIDasLong = messageID.toLong

  override def toString: String = s"[$distributingMode - $remoteAddress] $invocation"
}

object IFWorker {
  def apply(endpoint: String, serviceName: String = "", serviceObject: Any = None, interfaces: List[String] = Nil, timeout: Duration = 10 seconds) = new BlockingIFWorker(endpoint, serviceName, serviceObject, interfaces, timeout)

  def async(endpoint: String, serviceName: String = "", serviceObject: Any = None, interfaces: List[String] = Nil) = new AsyncIFWorker(endpoint, serviceName, serviceObject, interfaces)
}

class IFWorker(endpoint: String, serviceName: String = "", serviceObject: Any = None, interfaces: List[String] = Nil, val timeout: Duration = 10 seconds) {
  val isService = new AtomicBoolean(serviceName != "")
  val context = new ZContext()
  val socket = context.createSocket(SocketType.DEALER)
  socket.connect(endpoint)

  private val ioloopExecutor = Executors.newSingleThreadExecutor()
  private val hbExecutor = Executors.newSingleThreadExecutor()
  private val localInvokingExecutor = Executors.newSingleThreadExecutor()
  private val messageExpiration = new AtomicLong(2000)
  private val heartbeatInterval = IFDefinition.HEARTBEAT_LIVETIME / 5
  private val closed = new AtomicBoolean(false)
  private val sendingMessageQueue = new LinkedBlockingQueue[Message]()

  def send(msg: Message) = future(msg)

  ioloopExecutor.submit(new Runnable {
    private val charset = Charset.forName("UTF-8")

    override def run(): Unit = {
      while (!closed.get) {
        // sending
        try {
          while (sendingMessageQueue.size() > 0) {
            val msg = sendingMessageQueue.take()
            val zmsg = new ZMsg()
            zmsg.addLast("").addLast(msg.protocol).addLast(msg.messageID).addLast(msg.distributingMode).addLast(msg.remoteAddress).addLast(msg.serialization).add(msg.invocation.serialize(msg.serialization))
            Logging.debug("ready to send")
            val suc = zmsg.send(socket)
            Logging.debug("send Done.")
          }
        } catch {
          case e: Throwable => {
            if (!closed.get) {
              Logging.error("Error during sending message.", e)
            }
          }
        }
        // receiving
        try {
          val zmsg = ZMsg.recvMsg(socket, false)
          if (zmsg != null) {
            Logging.info((s"Received ${zmsg}"))
            if (zmsg.size() != 6) throw new IFException("Invalid Message from Broker.")
            val it = zmsg.iterator()
            it.next()
            assert(it.next().getString(charset) == IFDefinition.PROTOCOL)
            val messageID = it.next().getData
            val fromAddress = it.next().getData
            val serialization = it.next().getString(charset)
            val invocation = Invocation.deserialize(it.next().getData, serialization)
            val msg = Message.newFromBrokerMessage(new String(messageID, "UTF-8"), fromAddress, invocation, serialization)
            if (invocation.isRequest) onRequest(msg) else onResponse(msg)
          }
        } catch {
          case e: Throwable => {
            if (!closed.get) {
              Logging.error("Error during receiving message.", e)
            }
          }
        }
        // wait for next loop
        Thread.sleep(1)
      }
    }
  })
  hbExecutor.submit(new Runnable {
    private val needReReg = new AtomicBoolean(isService get)

    override def run(): Unit = {
      while (!closed.get) {
        try {
          if (needReReg.get) {
            blockingInvoker().registerAsService(serviceName, interfaces)
            needReReg set false
          }
          Thread.sleep(IFDefinition.HEARTBEAT_LIVETIME / 5)
          val hb = blockingInvoker("", timeout = (IFDefinition.HEARTBEAT_LIVETIME / 5) milliseconds).heartbeat().asInstanceOf[Boolean]
          if (!hb && isService.get) needReReg set true
        } catch {
          case e: Throwable => if (!closed.get) Logging.warning(s"Heartbeat: ${e.getMessage}")
        }
      }
    }
  })

  def toMessageInvoker(target: String = "") = new MessageRemoteObject(this, target)

  def asynchronousInvoker(target: String = "") = new AsynchronousRemoteObject(this, target)

  def blockingInvoker(target: String = "", timeout: Duration = 10 second) = new BlockingRemoteObject(this, target, timeout = timeout)

  def close() = {
    if (isService.get) {
      blockingInvoker().unregister()
      isService set false
    }
    closed set true
    socket.close()
    context.close()
    ioloopExecutor.shutdown()
    hbExecutor.shutdown()
    localInvokingExecutor.shutdown()
  }

  private class FutureEntry(var result: Option[Any] = None, var cause: Option[Throwable] = None)

  private object SingleThreadExecutionContext extends ExecutionContext with UncaughtExceptionHandler {
    private lazy val SingleThreadPool = Executors.newSingleThreadExecutor(new ThreadFactory() {
      private lazy val ThreadCount = new AtomicInteger(0)

      def newThread(runnable: Runnable): Thread = {
        val t = new Thread(runnable)
        t.setDaemon(true)
        t.setName(s"SingleThreadExecutionContextThread-${ThreadCount.getAndIncrement}")
        t.setUncaughtExceptionHandler(SingleThreadExecutionContext.this)
        t.setPriority(Thread.NORM_PRIORITY)
        t
      }
    })

    def execute(runnable: Runnable): Unit = {
      //      Logging.debug(s"submitting runnable: ${SingleThreadPool.isShutdown},${SingleThreadPool.isTerminated}")
      val f = SingleThreadPool.submit(runnable)
      Logging.debug(s"submitted. wait.")
      f.get
      Logging.debug(s"submitted. done.")
    }

    def reportFailure(cause: Throwable): Unit = {
      cause.printStackTrace()
    }

    def uncaughtException(thread: Thread, cause: Throwable): Unit = {
      cause.printStackTrace()
    }
  }

  private val waitingMap: mutable.HashMap[Long, (FutureEntry, Runnable)] = new mutable.HashMap

  private def future(msg: Message) = {
    val id = msg.messageIDasLong
    Logging.info(s"Sending ${msg}")
    val futureEntry = new FutureEntry
    Future[Any] {
      if (futureEntry.cause.isDefined) throw futureEntry.cause.get
      if (futureEntry.result.isDefined) futureEntry.result.get
      else throw new IFException("Error state: FutureEntry not defined.")
    }(new ExecutionContext {
      def execute(runnable: Runnable): Unit = {
        waitingMap.synchronized {
          if (waitingMap.contains(id)) throw new IFException("MessageID have been used.")
          waitingMap.put(id, (futureEntry, runnable))
        }
        sendingMessageQueue.offer(msg)
        //        SingleThreadExecutionContext.execute(() => {
        //          //          Logging.debug("outside submitted runnable")
        //          val zmsg = new ZMsg()
        //          zmsg.addLast("").addLast(msg.protocol).addLast(msg.messageID).addLast(msg.distributingMode).addLast(msg.remoteAddress).addLast(msg.serialization).add(msg.invocation.serialize(msg.serialization))
        //          Logging.debug("ready to send")
        //          val suc = zmsg.send(socket)
        //          Logging.debug("Runnable Done.")
        //        })
        //        //        Logging.debug("exe submitted in ec")
      }

      def reportFailure(cause: Throwable): Unit = {
        if (!closed.get) Logging.warning(s"nn:$cause", cause)
      }
    })
  }

  private def onRequest(msg: Message) = {
    val sourcePoint = msg.remoteAddress
    val invocation = msg.invocation
    localInvokingExecutor.submit(new Runnable {
      override def run(): Unit = {
        try {
          val result = if ("stopService" == invocation.getFunction) {
            isService set false
            asynchronousInvoker().unregister()
            "Stopped"
          } else invocation.perform(serviceObject)
          // val result = invocation.perform(serviceObject)
          send(Message.newDirectMessage(sourcePoint, Invocation.newResponse(msg.messageID, result)))
        } catch {
          case e: Throwable => send(Message.newDirectMessage(sourcePoint, Invocation.newError(msg.messageID, e.getMessage)))
        }
      }
    })
  }

  private def onResponse(msg: Message) = {
    val invocation = msg.invocation
    val id = invocation.getResponseID.toLong
    waitingMap.remove(id) match {
      case Some((futureEntry, runnable)) => {
        if (invocation.isError) futureEntry.cause = Some(new IFException(invocation.getError))
        else if (invocation.isResponse) futureEntry.result = Some(invocation.getResult)
        runnable.run()
      }
      case None => if (!closed.get) Logging.warning(s"MessageID not found: $id")
    }
  }
}

class BlockingIFWorker(endpoint: String, serviceName: String = "", serviceObject: Any = None, interfaces: List[String] = Nil, timeout: Duration = 10 seconds) extends IFWorker(endpoint, serviceName, serviceObject, interfaces, timeout) with Dynamic {
  def selectDynamic(targetName: String): BlockingRemoteObject = blockingInvoker(targetName)

  def applyDynamic(functionName: String)(args: Any*): Any = blockingInvoker("", timeout).applyDynamic(functionName)(args: _*)

  def applyDynamicNamed(functionName: String)(args: (String, Any)*): Any = blockingInvoker("", timeout).applyDynamicNamed(functionName)(args: _*)
}

class AsyncIFWorker(endpoint: String, serviceName: String = "", serviceObject: Any = None, interfaces: List[String] = Nil) extends IFWorker(endpoint, serviceName, serviceObject, interfaces, Duration.Zero) with Dynamic {
  def selectDynamic(targetName: String): AsynchronousRemoteObject = asynchronousInvoker(targetName)

  def applyDynamic(functionName: String)(args: Any*): Future[Any] = asynchronousInvoker("").applyDynamic(functionName)(args: _*)

  def applyDynamicNamed(functionName: String)(args: (String, Any)*): Future[Any] = asynchronousInvoker("").applyDynamicNamed(functionName)(args: _*)
}

class MessageRemoteObject(worker: IFWorker, targetName: String = "") extends Dynamic {
  def selectDynamic(functionName: String): Message = new InvokeItem(worker, targetName, functionName)().toMessage

  def applyDynamic(functionName: String)(args: Any*): Message = new InvokeItem(worker, targetName, functionName)(args.map(m => ("", m)): _*).toMessage

  def applyDynamicNamed(functionName: String)(args: (String, Any)*): Message = new InvokeItem(worker, targetName, functionName)(args: _*).toMessage
}

class BlockingRemoteObject(worker: IFWorker, targetName: String = "", timeout: Duration = 10 second) extends Dynamic {
  def selectDynamic(functionName: String): Any = new InvokeItem(worker, targetName, functionName)().requestMessage(timeout)

  def applyDynamic(functionName: String)(args: Any*): Any = new InvokeItem(worker, targetName, functionName)(args.map(m => ("", m)): _*).requestMessage(timeout)

  def applyDynamicNamed(functionName: String)(args: (String, Any)*): Any = new InvokeItem(worker, targetName, functionName)(args: _*).requestMessage(timeout)
}

class AsynchronousRemoteObject(worker: IFWorker, targetName: String = "") extends Dynamic {
  def selectDynamic(functionName: String): Future[Any] = new InvokeItem(worker, targetName, functionName)().sendMessage

  def applyDynamic(functionName: String)(args: Any*): Future[Any] = new InvokeItem(worker, targetName, functionName)(args.map(m => ("", m)): _*).sendMessage

  def applyDynamicNamed(functionName: String)(args: (String, Any)*): Future[Any] = new InvokeItem(worker, targetName, functionName)(args: _*).sendMessage
}

private class InvokeItem(worker: IFWorker, target: String, functionName: String)(args: (String, Any)*) {
  Logging.debug(("creating II"))
  val argsList: ArrayBuffer[Any] = new ArrayBuffer
  val namedArgsMap: mutable.HashMap[String, Any] = new mutable.HashMap
  args.foreach(m =>
    m match {
      case (name, value) if name == null || name.isEmpty => argsList += value
      case (name, value)                                 => namedArgsMap.put(name, value)
    }
  )
  Logging.debug(("II created"))
  val invocation = Invocation.newRequest(functionName, argsList.toList, namedArgsMap.toMap)

  def sendMessage = worker.send(toMessage)

  def requestMessage(timeout: Duration) = Await.result[Any](worker.send(toMessage), timeout)

  def toMessage = if (target == "") Message.newBrokerMessage(invocation) else Message.newServiceMessage(target, invocation)
}

object IFWorkerApp extends App {
  // val worker = IFWorker("tcp://172.16.60.199:224", "IFWorkerScalaTest")
  // //  val worker = IFWorker("tcp://127.0.0.1:224", "IFWorkerScalaTest")
  // Logging.info("Begin")
  // val random = new Random()
  // while (true) {
  //   Thread.sleep(400)
  //   val randomData = Range(0, 3).map(ch => Range(0, 10000).map(_ => random.nextDouble()).toArray).toArray
  //   worker.asynchronousInvoker("Storage").append("IFWorkerScalaLongTermTest", randomData, fetchTime = System.currentTimeMillis())
  //   //    println(worker.asynchronousInvoker("Storage").listServiceNames())
  //   Logging.debug(s"Looped at ${LocalDateTime.now()}")
  // }
  // worker.close()

  val brokerAddress = "tcp://172.16.60.200:224"
  val serviceName = "DSWorker1"
  val s1 = IFWorker(brokerAddress, serviceName = serviceName, serviceObject = "123")
  val client = IFWorker(brokerAddress)
  Thread.sleep(2000)
  // assert(client.listServiceNames().asInstanceOf[List[Any]].contains(serviceName))
  println(client.blockingInvoker(serviceName).stopService())

}
