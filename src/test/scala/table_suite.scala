package PGroups

import Datastore.CRDT.{DB, DT}
import Datastore.{Key, Store, MapStore}
import com.twitter.util.RandomSocket


object Util {

  def makeTable(id: String) = {
    val db = new MapStore[MemberList.T]()
    new Table(id, db)
  }

  import Enkidu.Node

  val lhost = "localhost"

  def nextNode() = {
     Node(lhost, RandomSocket.nextPort)
  }


}

