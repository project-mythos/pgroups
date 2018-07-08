package PGroups

import Enkidu.Mux.{TMSG, RMSG}
import Messages.{Request, Response}

import Datastore.Key 
import com.google.protobuf.ByteString
import Enki.PipeOps._

import Enkidu.Node

object Req {

  object Path {
    def apply(s: String): List[String] = s.split("/").toList
  }

  def key(req: Request) = Key.make(req.path)


  def encode(rep: Response): RMSG = {
    val payload = rep.toByteArray
    RMSG(payload)
  }



  def decode(msg: TMSG) = {
    Request.parseFrom(msg.payload)
  }

  def decode(msg: RMSG) = {
    Response.parseFrom( msg.payload )
  }


  def ok_rep(msg: String) = {
    Response(true, ByteString.copyFromUtf8(msg) )
  }



  

}




trait StringCodec[T] {
  def toString(t: T): String
  def fromString(s: String): T


  def toByteArray(t: T) = {
    toString(t).getBytes("utf8")
  }


  def toByteString(t: T) = {
    ByteString.copyFromUtf8( toString(t) )
  }


  def fromByteString(bytes: ByteString) = {
    bytes.toStringUtf8 |> fromString
  }

  def fromByteArray(bytes: Array[Byte]) = {
    new String(bytes, "utf8") |> fromString
  }


 
}


class SeqCodec[Elt](C: StringCodec[Elt]) extends StringCodec[Seq[Elt]] {

  type T = Seq[Elt]


  val delim = """(?<=\[)(.*?)(?=\])""".r






  private def encode(s: Elt) = "[" + C.toString(s) + "]"

  private def parse(s: String) = {
    delim.findAllIn(s).toVector
  }

  def toString(t: T) = {
    t.map( encode(_) ).mkString(" ")
  }

  def fromString(s: String) = {
    parse(s).map { x =>
      C.fromString(x)
    }
  }

}


object NodeCodec extends StringCodec[Node] {
  type T = Node

  def toString(t: T) = t.toString
  def fromString(s: String) = Node.fromString(s)
}


/*
object NodeList extends StringCodec[Set[Node]]{
  type T = Set[Node]

  def toString(t: T) = {
    t.map( Node.toString(_) ).mkString(",")
  }

  def fromString(s: String) = {
    s.split(",").map(x =>
      Node.fromString(x.trim)
    ).toSet
  }

}
 
 */



object NodeList extends SeqCodec(NodeCodec)
object NodeSet extends SetCodec(NodeCodec)

object KeyCodec extends StringCodec[Key] {
  def toString(key: Key) = {
    key.toString
  }

  def fromString(s: String) = Key.make(s)
}


object KeyList extends SeqCodec(KeyCodec)


class SetCodec[Elt](C: StringCodec[Elt]) extends StringCodec[ Set[Elt]]  {

  type T = Set[Elt]
  val SeqC = new SeqCodec(C)

  def fromString(s: String) = {
    SeqC.fromString(s).toSet 
  }

  def toString(t: T) = {
    SeqC.toString(t.toVector)
  }
}

