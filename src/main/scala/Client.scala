package PGroups

import Enkidu.Loadbalancer._

import Enkidu.Mux.{TMSG, RMSG}
import Enkidu.{ConnectionManager, Node, Connection}

import Legion.PeerService.ViewSync
import Enki.PipeOps._

import Enki_DT.ORSet
import com.twitter.util.{JavaTimer, Var, Await, Future}

import com.twitter.conversions.time._
import Messages.{Response, Request}

import Datastore.Key

import com.google.protobuf.ByteString

import Gossip.Messages.Node2







object Client {


  def apply(hosts: Set[String], CM: ConnectionManager[TMSG,RMSG]): Client = {

    val nodes = hosts.map( Node(_) )
    val SL = new ServerList(nodes)

    new Client(SL, CM)
  }

  def decode_set(rep: RMSG) = {
    Response.parseFrom(rep.payload).body |> NodeSet.fromByteString
  }



  /*** Builds a tmsg with attributes shared by all group update requests */
  def group_update_msg(path_s: String, key: Key, node: Node) = {
    val path = Req.Path(path_s)

    val payload =
      Request(key.toString, Node2.toByteString(node) ).toByteArray

    TMSG(path, payload)
  }



  def group_status_msg(path_s: String, key: Key) = {

    val path = Req.Path(path_s)

    val body = {
      val bs = ByteString.EMPTY
      Request(key.toString, bs).toByteArray
    }


    TMSG(path, body)
  }
}




class Client(SL: ServerList, CM: ConnectionManager[TMSG, RMSG]) {
  val LB = new LB(CM, P2C, SL)

  val timer = new JavaTimer() 

  import Client._ 


  def list_servers(): Future[Set[Node]] = {
    val req = TMSG(Req.Path("pgroups/list_servers") )

    LB.use {flow =>
      Connection.send_recv(flow, req)
    } map { rep =>

      ViewSync.decode(rep.payload).membership |> ORSet.query
    }

  }


  val interval = 1 second


  def maintain_servers() = {
    val seed = SL.endpoints.map(x => x.node).toSet

    val addr = Var(seed)

    def updater() = {
      list_servers() onSuccess {x => addr() = x } 
    }

    timer.schedule(interval)(updater)
    addr
  }





  def group_update(path: String, key: Key, node: Node) = {
    val req = group_update_msg(path, key, node)

    LB.use { flow =>
      Connection.send_recv(flow, req)
    } map(x => decode_set(x) )

  }



  def group_status(path_s: String, key: Key) = {
    val req = group_status_msg(path_s, key)
    LB.use {flow => Connection.send_recv(flow, req) }
  }


  def add(key: Key, node: Node) = {
    val path = "pgroups/add"
    group_update(path, key, node)
  }




  def remove(key: Key, node: Node) = {
    val path = "pgroups/remove"
    group_update(path, key, node)
  }




  def create(key: Key) = {
    val path = "pgroups/create"
    group_status(path, key)
  }


  def delete(key: Key) = {
    val path = "pgroups/delete"
    group_status(path, key)
  }





  def list(key: Key) = {

    val path = "pgroups/list"

    group_status(path, key) map {x =>
      NodeSet.fromByteArray(x.payload).toSet 
    }

  }



  // might have to block at initialization
  def resolve(key: Key): Var[ Set[Node] ] = {
    val hosts = Await.result( list(key) )
    val addr = Var(hosts)

    def updater() = {
      list(key) onSuccess {eps => addr() = eps}
    }

    timer.schedule(interval)(updater)
    addr
  }


 
  def groups() = {
    val req = TMSG("pgroups/groups")

    LB.use {flow => Connection.send_recv(flow, req) } map {rep =>
      KeyList.fromByteArray(rep.payload)
    }

  }

}


