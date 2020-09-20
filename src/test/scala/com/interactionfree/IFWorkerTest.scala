package com.interactionfree

import java.io.IOError

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class IFWorkerTest extends AnyFunSuite with BeforeAndAfter {
  val testPort = 224
  val brokerAddress = s"tcp://127.0.0.1:${testPort}"
  val tobeClosedWorkers = new ListBuffer[IFWorker]()

  test("Test Dynamic Invoker.") {
    val worker = new IFWorker(brokerAddress)
    val invoker = worker.toMessageInvoker()
    val m1 = invoker.fun1(1, 2, "3", b = None, c = List(1, 2, "3d"))
    assert(m1.isBrokerMessage)
    val m1Invocation = m1.invocation
    assert(m1Invocation.isRequest)
    assert(m1Invocation.getFunction == "fun1")
    assert(m1Invocation.getArguments == List(1, 2, "3"))
    assert(m1Invocation.getKeywordArguments == Map("b" -> None, "c" -> List(1, 2, "3d")))
    val invoker2 = worker.toMessageInvoker("OnT")
    val m2 = invoker2.fun2()
    val m2Invocation = m2.invocation
    assert(m2Invocation.isRequest)
    assert(m2Invocation.getFunction == "fun2")
    assert(m2Invocation.getArguments == Nil)
    assert(m2Invocation.getKeywordArguments == Map())
    assert(m2.isServiceMessage)
    assert(m2.remoteAddress sameElements "OnT".getBytes("UTF-8"))
    worker.close()
  }

  test("Test Remote Invoke And Async.") {
    val worker1 = new IFWorker(brokerAddress)
    val invoker1 = worker1.asynchronousInvoker()
    val future1 = invoker1.co()
    try {
      Await.result(future1, 10 seconds)
      throw new RuntimeException
    } catch {
      case e: IFException => assert(e.getMessage == "Function [co] not available.")
    }
    val future2 = invoker1.protocol(a = 1, b = 2)
    try {
      Await.result(future2, 1 seconds)
      throw new RuntimeException
    } catch {
      case e: IFException => assert(e.getMessage == "Keyword Argument [a] not availabel for function [protocol].")
    }
    val future3 = invoker1.protocol(1, 2, 3)
    try {
      Await.result(future3, 1 seconds)
      throw new RuntimeException
    } catch {
      case e: IFException => assert(e.getMessage == "Function [protocol] expects [0] arguments, but [3] were given.")
    }
    val future4 = invoker1.protocol()
    assert(Await.result(future4, 1 seconds) == "IF1")
    worker1.close()
  }

  test("Test Remote Invoke And Sync.") {
    val worker = IFWorker(brokerAddress)
    val invoker = worker.blockingInvoker()
    try {
      invoker.co()
      throw new RuntimeException
    } catch {
      case e: IFException => assert(e.getMessage == "Function [co] not available.")
    }
    assert(invoker.protocol() == "IF1")
    worker.close()
  }

  test("Test Sync And Async Mode.") {
    val workerB = IFWorker(brokerAddress)
    try {
      workerB.co()
      throw new RuntimeException
    } catch {
      case e: IFException => assert(e.getMessage == "Function [co] not available.")
    }
    assert(workerB.protocol() == "IF1")
    val workerA = IFWorker.async(brokerAddress)
    val future1 = workerA.protocol(1, 2, 3)
    try {
      Await.result(future1, 1 seconds)
      throw new RuntimeException
    } catch {
      case e: IFException => assert(e.getMessage == "Function [protocol] expects [0] arguments, but [3] were given.")
    }
    workerB.close()
    workerA.close()
  }

  test("Test Invoke Other Client.") {
    class Target(var notFunction: Int = 100) {
      def v8 = "V8 great!"

      def v9() = throw new IFException("V9 not good.")

      def v10() = throw new IOError(new RuntimeException("V10 have problems."))

      def v(i: Int, b: Boolean) = "OK"
    }
    val worker1 = IFWorker(brokerAddress, serviceObject = new Target(), serviceName = "T1_Benz")
    val checker = IFWorker(brokerAddress, timeout = 1 second)
    Thread.sleep(1000)
    try {
      val benzChecker = checker.T1_Benz
      assert(benzChecker.v8() == "V8 great!")
      try {
        benzChecker.v9()
        assert(false)
      } catch {
        case e: IFException => assert(e.getMessage == "Failed to invoke v9: V9 not good.")
      }
      try {
        benzChecker.v10()
        assert(false)
      } catch {
        case e: IFException => assert(e.getMessage == "Failed to invoke v10: java.lang.RuntimeException: V10 have problems.")
      }
      assert(benzChecker.v(1, false) == "OK")
      try {
        benzChecker.v11()
        assert(false)
      } catch {
        case e: IFException => assert(e.getMessage == "Function not valid: v11.")
      }
      assert(benzChecker.notFunction == 100)
    }
    finally {
      worker1.close()
      checker.close()
    }
  }

  test("Test Service Duplicated.") {
    val worker1 = IFWorker(brokerAddress, serviceName = "T2-ClientDuplicated")
    Thread.sleep(1000)
    try {
//      val worker2 = IFWorker(brokerAddress, serviceName = "T2-ClientDuplicated")
//      assert(false)
    } catch {
      case e: IFException => assert(e.getMessage == "Service name [T2-ClientDuplicated] occupied.")
    }
    worker1.close()
  }
}
