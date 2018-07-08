package PGroups

import Datastore.CRDT.{DB, DT}
import Datastore.{Key, Store, BatchStore}

import Enkidu.Node
import Enki_DT.ORSet

import com.twitter.util.{Var, JavaTimer, Duration}
import Enki.PipeOps._
import Enki_DT.Proto.ORSetBridge
import Gossip.Messages.Node2
import Legion.AntiEntropy.Sync

import Enki_DT.Proto.{PBBridge}
import Gossip.{Messages => GMSG}
import com.google.protobuf._ 

object MemberList extends ORSetBridge(Node2)  {
  type T = ORSet[Node]
}


case class MemberList(id: String) extends DT[ ORSet[Node] ] {
  type T = ORSet[Node]


  def localize(os: T) = {
    val ndot = os.local.copy(id=id)
    os.copy(local = ndot) 
  }


  def merge(l: T, r: T) = ORSet.merge(l, r)
  def zero() = ORSet.empty[Node](id)

  def encode(t: T) = MemberList.encode(t)
  def decode(bytes: Array[Byte]) = MemberList.decode(bytes)

}





class Table(id: String, val S: Store[MemberList.T]) {

  val ML = MemberList(id)
  val STORE = new DB(S, ML)

  type MList = MemberList.T


  val timer = new JavaTimer() 

  def add(k: Key, v: Node) = {
    STORE.update(k) {os => ORSet.add(os, v)}
  }

  def remove(k: Key, v: Node) = {
    STORE.update(k) {os => ORSet.delete(os, v) }
  }


  def delete(k: Key) = {
    STORE.delete(k) 
  }



  import Types.TBLMap
  def merge(l: TBLMap, r: TBLMap) = {

    STORE.merge(l, r)
  }


  def query(k: Key) = {
    STORE.get(k).getOrElse(ML.zero) |> ORSet.query
  }

  def create(k: Key) = {

    if (STORE.exists(k)) {
      STORE.put(k, ML.zero)
    }

  }






  def resolve(key: Key, ttl: Duration): Var[Set[Node]] = {
    val q = query(key)
    val addr = Var(q)

    def updater() = {
     addr() = query(key)
    }

    timer.schedule(ttl)(updater)
    addr 
  }


  def resolve(k: String, ttl: Duration): Var[Set[Node]] = {
    resolve(Key.make(k), ttl)
  }


  def resolve[T](k: T, ttl: Duration): Var[Set[Node]] = {
    k match {
      case key: String => resolve(key, ttl)
      case key: Key => resolve(key, ttl)
      case _ => throw new IllegalArgumentException("Unknown Type")
    }
  }


  def toMap() = STORE.toMap 

}



object Types {
  type TBLMap = Map[Key, ORSet[Node]]
}


//extends Sync[Types.TBLMap] with ProtoBridge[TBLMap ]

import Types._ 

object Table extends PBBridge[TBLMap, Messages.TBL] {




  val companion = Messages.TBL

  def toPB(t: TBLMap) = {
    val state = t.map {case (k, v) =>
      (k.toString -> MemberList.toByteString(v) )
    }

    Messages.TBL(state)
  }


  def fromPB(tbl: Messages.TBL) = {
    tbl.state map {case (k, v) =>
      (Key.make(k), MemberList.decode(v) )
    }
  }
}


