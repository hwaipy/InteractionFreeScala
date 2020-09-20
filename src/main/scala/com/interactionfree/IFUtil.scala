package com.interactionfree

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.math.BigInteger
import java.time.LocalDateTime
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import org.msgpack.value.Value

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

object NumberTypeConversions {
  implicit def anyToLong(x: Any): Long = {
    x match {
      case x: Byte => x.toLong
      case x: Short => x.toLong
      case x: Int => x.toLong
      case x: Long => x.toLong
      case x: String => x.toLong
      case _ => throw new IllegalArgumentException(s"$x can not be cast to Long.")
    }
  }

  implicit def anyToInt(x: Any): Int = {
    x match {
      case x: Byte => x.toInt
      case x: Short => x.toInt
      case x: Int => x.toInt
      case x: Long => x.toInt
      case x: String => x.toInt
      case _ => throw new IllegalArgumentException(s"$x can not be cast to Int.")
    }
  }

  implicit def anyToDouble(x: Any): Double = {
    x match {
      case x: Byte => x.toDouble
      case x: Short => x.toDouble
      case x: Int => x.toDouble
      case x: Long => x.toDouble
      case x: Float => x.toDouble
      case x: Double => x.toDouble
      case x: String => x.toDouble
      case _ => throw new IllegalArgumentException(s"$x can not be cast to Int.")
    }
  }
}

object Logging {
  val startTime = System.currentTimeMillis()
  val index = new AtomicInteger()
//  var out = new PrintWriter("log/logging-" + startTime + "-" + index.get)
  val currentCount = new AtomicInteger(0)

  object Level extends Enumeration {
    type Level = Value
    val Error, Warning, Info, Debug = Value
  }

  import Level._

  def log(level: Level, msg: String, exception: Throwable = null) = {
//    println(s"${LocalDateTime.now()} [${level}] [${Thread.currentThread().getName}] $msg")
//    out.println(s"${LocalDateTime.now()} [${level}] [${Thread.currentThread().getName}] $msg")
    if (exception != null) {
      exception.printStackTrace()
//      exception.printStackTrace(out)
    }
//    out.flush()
    if (currentCount.incrementAndGet() > 10000) {
      index.incrementAndGet()
//      out.close()
//      out = new PrintWriter("log/logging-" + startTime + "-" + index.get)
      currentCount set 0
    }
  }

  def error(msg: String, exception: Throwable = null) = log(Error, msg, exception)

  def warning(msg: String, exception: Throwable = null) = log(Warning, msg, exception)

  def info(msg: String, exception: Throwable = null) = log(Info, msg, exception)

  def debug(msg: String, exception: Throwable = null) = log(Debug, msg, exception)
}

object MsgpackSerializer {
  protected[interactionfree] val maxDeepth = 100
  def serialize(content: Any) = {
      val packer = org.msgpack.core.MessagePack.newDefaultBufferPacker

      def doFeed(value: Any, deepth: Int = 0, target: Option[String] = null): Unit = {
        if (deepth > MsgpackSerializer.maxDeepth) {
          throw new IFException("Message over deepth.")
        }
        value match {
          case n if n == null || n == None => packer.packNil
          case i: Int => packer.packInt(i)
          case i: AtomicInteger => packer.packInt(i.get)
          case s: String => packer.packString(s)
          case b: Boolean => packer.packBoolean(b)
          case b: AtomicBoolean => packer.packBoolean(b.get)
          case l: Long => packer.packLong(l)
          case l: AtomicLong => packer.packLong(l.get)
          case s: Short => packer.packShort(s)
          case c: Char => packer.packShort(c.toShort)
          case b: Byte => packer.packByte(b)
          case f: Float => packer.packFloat(f)
          case d: Double => packer.packDouble(d)
          case bi: BigInteger => packer.packBigInteger(bi)
          case bi: BigInt => packer.packBigInteger(bi.bigInteger)
          case bytes: Array[Byte] => {
            packer.packBinaryHeader(bytes.length)
            packer.writePayload(bytes)
          }
          case array: Array[_] => {
            packer.packArrayHeader(array.length)
            for (i <- Range(0, array.length)) {
              doFeed(array(i), deepth + 1, target)
            }
          }
          case seq: Seq[_] => {
            packer.packArrayHeader(seq.size)
            seq.foreach(i => doFeed(i, deepth + 1, target))
          }
          case set: Set[_] => {
            packer.packArrayHeader(set.size)
            set.foreach(i => doFeed(i, deepth + 1, target))
          }
          case set: java.util.Set[_] => {
            packer.packArrayHeader(set.size)
            val it = set.iterator
            while (it.hasNext) {
              doFeed(it.next, deepth + 1, target)
            }
          }
          case list: java.util.List[_] => {
            packer.packArrayHeader(list.size)
            val it = list.iterator
            while (it.hasNext) {
              doFeed(it.next, deepth + 1, target)
            }
          }
          case map: scala.collection.Map[_, Any] => {
            packer.packMapHeader(map.size)
            map.foreach(entry => {
              doFeed(entry._1, deepth + 1, target)
              doFeed(entry._2, deepth + 1, target)
            })
          }
          case map: java.util.Map[_, _] => {
            packer.packMapHeader(map.size)
            val it = map.entrySet.iterator
            while (it.hasNext) {
              val entry = it.next
              doFeed(entry.getKey, deepth + 1, target)
              doFeed(entry.getValue, deepth + 1, target)
            }
          }
          case unit if (unit == () || unit == scala.runtime.BoxedUnit.UNIT) => packer.packNil
          case p: Product => {
            packer.packArrayHeader(p.productArity)
            val it = p.productIterator
            while (it.hasNext) {
              doFeed(it.next(), deepth + 1, target)
            }
          }
          case _ => throw new IFException(s"Unrecognized value: ${value}")
        }
      }

      doFeed(content)
      packer.toByteArray
  }

  def deserialize(bytes: Array[Byte]) = {
      val unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(bytes)
      val value = unpacker.unpackValue()
      convert(value)
  }

  private def convert(value: Value, deepth: Int = 0): Any = {
    if (deepth > maxDeepth) throw new IllegalArgumentException("Message over deepth.")
    import org.msgpack.value.ValueType._
    value.getValueType match {
      case ARRAY => {
        val arrayValue = value.asArrayValue
        val list: ListBuffer[Any] = ListBuffer()
        val it = arrayValue.iterator
        while (it.hasNext) {
          list += convert(it.next, deepth + 1)
        }
        list.toList
      }
      case MAP => {
        val mapValue = value.asMapValue
        val map: mutable.HashMap[Any, Any] = new mutable.HashMap
        val it = mapValue.entrySet.iterator
        while (it.hasNext) {
          val entry = it.next
          map += (convert(entry.getKey, deepth + 1) -> convert(entry.getValue, deepth + 1))
        }
        map.toMap
      }
      case BINARY => value.asBinaryValue.asByteArray
      case BOOLEAN => value.asBooleanValue.getBoolean
      case FLOAT => value.asFloatValue.toDouble
      case INTEGER => {
        val integerValue = value.asIntegerValue
        if (integerValue.isInLongRange) {
          if (integerValue.isInIntRange) {
            integerValue.toInt
          } else {
            integerValue.toLong
          }
        } else {
          BigInt.javaBigInteger2bigInt(integerValue.asBigInteger)
        }
      }
      case NIL => None
      case STRING => value.asStringValue.toString
      case _ => throw new IllegalArgumentException(s"Unknown ValueType: ${value.getValueType}")
    }
  }
}