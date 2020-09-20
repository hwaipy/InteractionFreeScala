package com.interactionfree

import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import org.msgpack.core.MessagePack
import org.msgpack.value.Value
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import com.interactionfree.MsgpackSerializer

object Invocation {
  val KeyType = "Type"
  val KeyFunciton = "Function"
  val KeyArguments = "Arguments"
  val KeyKeyworkArguments = "KeyworkArguments"
  val KeyRespopnseID = "ResponseID"
  val KeyResult = "Result"
  val KeyError = "Error"
  val KeyWarning = "Warning"
  val ValueTypeRequest = "Request"
  val ValueTypeResponse = "Response"
  val Preserved = List(KeyType, KeyFunciton, KeyArguments, KeyKeyworkArguments, KeyRespopnseID, KeyResult, KeyError, KeyWarning)

  def newRequest(functionName: String, args: List[Any] = Nil, kwargs: Map[String, Any] = Map()) = new Invocation(Map(
    Invocation.KeyType -> ValueTypeRequest,
    Invocation.KeyFunciton -> functionName,
    Invocation.KeyArguments -> args,
    Invocation.KeyKeyworkArguments -> kwargs
  ))

  def newResponse(messageID: String, result: Any) = new Invocation(Map(
    Invocation.KeyType -> Invocation.ValueTypeResponse,
    Invocation.KeyRespopnseID -> messageID,
    Invocation.KeyResult -> result
  ))

  def newError(messageID: String, description: String) = new Invocation(Map(
    Invocation.KeyType -> Invocation.ValueTypeResponse,
    Invocation.KeyRespopnseID -> messageID,
    Invocation.KeyError -> description
  ))

  def deserialize(bytes: Array[Byte], serialization: String = "Msgpack") = serialization match {
    case "Msgpack" => {
      val map = MsgpackSerializer.deserialize(bytes).asInstanceOf[Map[String, Any]]
      new Invocation(map)
    }
    case _ => throw new IFException(s"Bad serialization: ${serialization}")
  }
}

class Invocation(private val content: Map[String, Any]) {

  def get(key: String) = content.get(key)

  def apply(key: String) = content(key)

  def isRequest = content(Invocation.KeyType) == Invocation.ValueTypeRequest

  def isResponse = content(Invocation.KeyType) == Invocation.ValueTypeResponse

  def isError = isResponse && content.contains(Invocation.KeyError)

  def hasWarning = isResponse && content.contains(Invocation.KeyWarning)

  def getFunction = if (isRequest) content(Invocation.KeyFunciton).asInstanceOf[String] else throw new IFException("Not a Request.")

  def getArguments = if (isRequest) content.get(Invocation.KeyArguments) match {
    case Some(args) => args match {
      case a: List[_] => a
      case a => List(a)
    }
    case None => Nil
  } else throw new IFException("Not a Request.")

  def getKeywordArguments = if (isRequest) content.get(Invocation.KeyKeyworkArguments) match {
    case Some(kwargs) => kwargs.asInstanceOf[Map[String, Any]]
    case None => Map[String, Any]()
  } else throw new IFException("Not a Request.")

  def getResult = if (isResponse && !isError) content(Invocation.KeyResult) else throw new IFException("Not a valid Response.")

  def getWarning = if (isResponse) content(Invocation.KeyWarning).asInstanceOf[String] else throw new IFException("Not a Response.")

  def getError = if (isError) content(Invocation.KeyError).asInstanceOf[String] else throw new IFException("Not an Error.")

  def getResponseID = if (isResponse) content(Invocation.KeyRespopnseID).asInstanceOf[String] else throw new IFException("Not a Response.")

  def serialize(serialization: String = "Msgpack") = serialization match {
    case "Msgpack" => MsgpackSerializer.serialize(content)
    case _ => throw new IFException(s"Bad serialization: ${serialization}")
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[Invocation]) false
    eq(obj.asInstanceOf[Invocation].content, content)
  }

  override def toString: String =
    if (isRequest) s"Request: ${getFunction} $getArguments $getKeywordArguments"
    else if (isError) s"Error ${new String(getResponseID)} $getError"
    else s"Response ${new String(getResponseID)} $getResult"

  def perform(target: Any) = {
    require(isRequest)
    val name = getFunction
    val args = getArguments
    val kwargs = getKeywordArguments
    val members = currentMirror.classSymbol(target.getClass).toType.members.collect {
      case m: MethodSymbol => m
    }
    val instanceMirror = currentMirror.reflect(target)
    val typeSignature = instanceMirror.symbol.typeSignature
    val methods = members.collect {
      case m if (m.name.toString == name) => matchParameters(m, args, kwargs, instanceMirror, typeSignature)
    }
    if (methods.size == 0) throw new IFException(s"Function not valid: ${name}.")
    val valids = methods.collect {
      case m: Option[(Any, Any)] if m != None => m
    }
    valids.size match {
      case 0 => throw new IFException(s"Function not valid: ${name}.")
      case _ => {
        val methodInstance = instanceMirror.reflectMethod(valids.head.get._1)
        try methodInstance(valids.head.get._2: _*)
        catch {
          case e: Throwable => throw new IFException(s"Failed to invoke ${name}: ${e.getCause.getMessage}", e)
        }
      }
    }
  }

  private def matchParameters(method: MethodSymbol, args: List[Any], kwargs: Map[String, Any], instanceMirror: InstanceMirror, typeSignature: Type) = {
    val paramInfo = (for (ps <- method.paramLists; p <- ps) yield p).zipWithIndex.map({ p =>
      (p._1.name.toString, p._1.asTerm.isParamWithDefault,
        typeSignature member TermName(s"${method.name}$$default$$${p._2 + 1}") match {
          case defarg if defarg != NoSymbol => Some((instanceMirror reflectMethod defarg.asMethod) ())
          case _ => None
        })
    }).drop(args.size)
    val paramMatches = paramInfo.collect {
      case info if kwargs.contains(info._1) => Some(kwargs(info._1))
      case info if info._2 => info._3
      case info => None
    }
    paramMatches.contains(None) match {
      case true => None
      case false => Some((method, args ::: paramMatches.map(p => p.get)))
    }
  }

  private def eq(a: Any, b: Any): Boolean = {
    def tryWrapInList(v: Any): Option[List[Any]] = {
      val list: ListBuffer[Any] = ListBuffer()
      v match {
        case array: Array[_] => {
          for (i <- Range(0, array.length)) {
            list += array(i)
          }
        }
        case seq: Seq[_] => list.appendAll(seq)
        case set: Set[_] => list.appendAll(set)
        case set: java.util.Set[_] => {
          val it = set.iterator
          while (it.hasNext) {
            list += it.next
          }
        }
        case l: java.util.List[_] => {
          val it = l.iterator
          while (it.hasNext) {
            list += it.next
          }
        }
        case _ => return None
      }
      Some(list.toList)
    }

    def tryWrapInMap(v: Any): Option[Map[String, Any]] = {
      val hm: collection.mutable.HashMap[String, Any] = collection.mutable.HashMap()
      v match {
        case map: scala.collection.Map[_, Any] => map.foreach(entry => {
          hm += (entry._1.toString -> entry._2)
        })
        case map: java.util.Map[_, _] => {
          val it = map.entrySet.iterator
          while (it.hasNext) {
            val entry = it.next
            hm += (entry.getKey.toString -> entry.getValue)
          }
        }
        case _ => return None
      }
      Some(hm.toMap)
    }

    def mapEq(mapA: Map[String, Any], mapB: Map[String, Any]): Boolean = {
      if (mapA.size != mapB.size) return false
      for ((k, v) <- mapA) {
        if (!mapB.contains(k)) return false
        if (!eq(v, mapB.get(k).get)) return false
      }
      return true
    }

    val listSomeA = tryWrapInList(a)
    val listSomeB = tryWrapInList(b)
    val mapSomeA = tryWrapInMap(a)
    val mapSomeB = tryWrapInMap(b)
    if (listSomeA != None && listSomeB != None) {
      return listSomeA.get == listSomeB.get
    }
    if (mapSomeA != None && mapSomeB != None) {
      return mapEq(mapSomeA.get, mapSomeB.get)
    }
    if ((a == None || a == null || a == ()) && (b == None || b == null || b == ())) return true
    if ((a.isInstanceOf[BigInteger] || a.isInstanceOf[BigInt]) && (b.isInstanceOf[BigInteger] || b.isInstanceOf[BigInt])) {
      return (a match {
        case bi: BigInteger => BigInt.javaBigInteger2bigInt(bi)
        case bi: BigInt => bi
      }) == (b match {
        case bi: BigInteger => BigInt.javaBigInteger2bigInt(bi)
        case bi: BigInt => bi
      })
    }
    return a == b
  }
}
