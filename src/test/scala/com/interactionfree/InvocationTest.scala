package com.interactionfree

import java.math.BigInteger
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class InvocationTest extends AnyFunSuite with BeforeAndAfter {
  val sampleRequestContent = Map[String, Any](
    Invocation.KeyType -> Invocation.ValueTypeRequest,
    Invocation.KeyFunciton -> "FunctionName",
    Invocation.KeyArguments -> List(1, "ARG2", Nil),
    Invocation.KeyKeyworkArguments -> Map("A" -> "a", "B" -> 2, "D" -> null, "C" -> -0.4),
    "keyString" -> "value1",
  )
  val sampleResponseContent = Map[String, Any](
    Invocation.KeyType -> Invocation.ValueTypeResponse,
    Invocation.KeyRespopnseID -> "ID1",
    Invocation.KeyResult -> List(1, "Res"),
    Invocation.KeyWarning -> "Something not good"
  )
  val sampleErrorContent = Map[String, Any](
    Invocation.KeyType -> Invocation.ValueTypeResponse,
    Invocation.KeyRespopnseID -> "12345",
    Invocation.KeyResult -> List(1, "Res"),
    Invocation.KeyWarning -> "Something not good",
    Invocation.KeyError -> "Fatal Error!"
  )
  val mapIn = Map[String, Any](
    "keyString" -> "value1",
    "keyInt" -> 123,
    "keyLong" -> (Int.MaxValue.toLong + 100),
    "keyBigInteger" -> new BigInteger(s"${Long.MaxValue}").add(new BigInteger(s"${Long.MaxValue}")),
    "keyBooleanFalse" -> false,
    "KeyBooleanTrue" -> true,
    "keyByteArray" -> Array[Byte](1, 2, 2, 2, 2, 1, 1, 2, 2, 1, 1, 4, 5, 4, 4, -1),
    "keyIntArray" -> Array[Int](3, 526255, 1321, 4, -1),
    "keyNull" -> null,
    "keyLongMax" -> Long.MaxValue,
    "keyLongMin" -> Long.MinValue,
    "keyDouble" -> 1.242,
    "keyDouble2" -> -12.2323e-100,
    "keyDouble3" -> Double.MaxValue,
    "keyDouble4" -> Double.MinPositiveValue,
    "keyBigInteger2" -> BigInteger.valueOf(Long.MaxValue).add(BigInteger.TEN),
    "keyUnit" -> ()
  )
  val map = Map[String, Any]("keyMap" -> mapIn)

  before {
  }

  test("Test Get Information") {
    val m = new Invocation(sampleRequestContent)
    assert(m.get("keyString") == Some("value1"))
    assert(m("keyString") == "value1")
  }

  test("Test Type And Content") {
    val m1 = new Invocation(sampleRequestContent)
    assert(m1.isRequest)
    assert(m1.getFunction == "FunctionName")
    assert(m1.getArguments == List(1, "ARG2", Nil))
    assert(m1.getKeywordArguments == Map("A" -> "a", "B" -> 2, "C" -> -0.4, "D" -> null))

    val m2 = new Invocation(sampleResponseContent)
    assert(m2.isResponse)
    assert(!m2.isError)
    assert(m2.hasWarning)
    assert(m2.getResult == List(1, "Res"))
    assert(m2.getWarning == "Something not good")
    assert(m2.getResponseID sameElements "ID1".getBytes("UTF-8"))

    val m3 = new Invocation(sampleErrorContent)
    assert(m3.isResponse)
    assert(m3.isError)
    assert(m3.hasWarning)
    assert(m3.getWarning == "Something not good")
    assert(m3.getError == "Fatal Error!")
    intercept[IFException] {
      m3.getResult
    }
    assert(m3.getResponseID == "12345")
  }

    test("Test over deepth") {
      var odMap = Map[String, Any]()
      for (d <- Range(0, 200)) {
        odMap = Map("item" -> odMap)
      }
      try {
        new Invocation(odMap).serialize()
        assert(false)
      } catch {
        case e: IFException => assert(e.getMessage == "Message over deepth.")
        case e: Throwable => assert(false)
      }
    }

    test("Test Map pack and unpack") {
      val invocation1 = new Invocation(map)
      val invocation2 = Invocation.deserialize(invocation1.serialize())
      assert(invocation1 == invocation2)
    }

    test("Test Invalid Parameters") {
      intercept[IFException] {
        Invocation.newRequest(null, kwargs = Map()).perform(new InvokerTestClass())
      }
      intercept[IFException] {
        Invocation.newRequest("null", Nil, null).perform(new InvokerTestClass())
      }
      intercept[IFException] {
        Invocation.newRequest("null", null, Map()).perform(new InvokerTestClass())
      }
      intercept[IFException] {
        Invocation.newRequest("null", null, null).perform(new InvokerTestClass())
      }
      intercept[IFException] {
        Invocation.newRequest(null, null, null).perform(new InvokerTestClass())
      }
    }

    test("Test Invoke None") {
      intercept[IFException] {
        Invocation.newRequest("runf").perform(new InvokerTestClass())
      }
      intercept[IFException] {
        Invocation.newRequest("run", kwargs = Map("asd" -> 3)).perform(new InvokerTestClass())
      }
    }

    test("Test Method 1") {
      assert(Invocation.newRequest("run", kwargs = Map("a1" -> 1)).perform(new InvokerTestClass()) == "Method 1:1")
      intercept[IFException] {
        Invocation.newRequest("run", kwargs = Map("a1" -> -1)).perform(new InvokerTestClass())
      }
    }

    test("Test Method 2") {
      assert(Invocation.newRequest("run", kwargs = Map("a2" -> 1, "b2" -> 1.1)).perform(new InvokerTestClass()) == "Method 2:1,1.1")
    }

    test("Test Method 3") {
      assert(Invocation.newRequest("run", kwargs = Map("a3" -> 1, "b3" -> 1.1, "c3" -> List("1"), "d3" -> false)).perform(new InvokerTestClass()) == "Method 3:1,1.1,List(1),false")
      assert(Invocation.newRequest("run", kwargs = Map("a3" -> 1, "b3" -> 1.1, "c3" -> Vector("1"), "d3" -> false)).perform(new InvokerTestClass()) == "Method 3:1,1.1,Vector(1),false")
      intercept[IFException] {
        Invocation.newRequest("run", kwargs = Map("a3" -> 1, "b3" -> 1.1, "c3" -> List(1), "d3" -> false)).perform(new InvokerTestClass())
      }
    }

    test("Test No Param Method") {
      assert(Invocation.newRequest("noParam").perform(new InvokerTestClass()) == "No Param")
      assert(Invocation.newRequest("noParam", args = Nil).perform(new InvokerTestClass()) == "No Param")
      assert(Invocation.newRequest("noParam", kwargs = Map()).perform(new InvokerTestClass()) == "No Param")
    }

    test("Test hybrid invoke.") {
      assert(Invocation.newRequest("func", kwargs = Map("a1" -> 100, "a2" -> "hello", "a3" -> 0.5, "a4" -> 100.1010f, "a5" -> true)).perform(new InvokerTestClass()) == "FUNC:100,hello,0.5,100.101,true")
      assert(Invocation.newRequest("func", kwargs = Map("a1" -> 100, "a2" -> "hello", "a3" -> 0.5, "a5" -> true)).perform(new InvokerTestClass()) == "FUNC:100,hello,0.5,1.0,true")
      assert(Invocation.newRequest("func", kwargs = Map("a1" -> 100, "a2" -> "hello", "a3" -> 0.5)).perform(new InvokerTestClass()) == "FUNC:100,hello,0.5,1.0,false")
      assert(Invocation.newRequest("func", 101 :: "hi" :: Nil, Map("a1" -> 100, "a2" -> "hello", "a3" -> 0.5)).perform(new InvokerTestClass()) == "FUNC:101,hi,0.5,1.0,false")
    }

  class InvokerTestClass {
    def run(a1: Int) = {
      a1 match {
        case a1 if a1 < 0 => {
          throw new RuntimeException()
        }
        case _ => s"Method 1:$a1"
      }
    }

    def run(a2: Int, b2: Double) = s"Method 2:$a2,$b2"

    def run(a3: Int = 100, b3: Double = 1.0, c3: Seq[String], d3: Boolean) = {
      val s: String = c3.head
      s"Method 3:$a3,$b3,$c3,$d3"
    }

    def noParam = "No Param"

    def func(a1: Int, a2: String, a3: Double = 0.1, a4: Float = 1, a5: Boolean = false) = s"FUNC:$a1,$a2,$a3,$a4,$a5"
  }

}
