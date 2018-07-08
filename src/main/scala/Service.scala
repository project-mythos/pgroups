package PGroups

//import Enkidu.{Mux, Listener}
import com.twitter.util.{Future, Duration}
import Legion.{View, PeerService}
import PeerService.Server

import Enkidu.Mux.{TMSG, RMSG}
import Messages.{Request, Response}

import Datastore.Key 
import com.google.protobuf.ByteString
import Enki.PipeOps._

import Enkidu.{Node, Flow}

import Gossip.Messages.{Node2, Rumor}
import Legion.AntiEntropy.{Sync, Contents}

import Enki_DT.ORSet
import Types.TBLMap



class TableContents(Tbl: Table) extends Contents[TBLMap] {
  def encode(t: TBLMap) = Table.encode(t)
  def decode(bytes: Array[Byte]) = Table.decode(bytes)

  def merge(l: TBLMap, r: TBLMap) = {
    Tbl.merge(l, r)
  }

}



class Endpoints(Tbl: Table) {

  def update_group(req: Request)(f: (Key, Node) => ORSet[Node]) = {

    val key = Req.key(req)
    val node = Node2.decode( req.body )

    val body =
      f(key, node) |> ORSet.query |> NodeSet.toByteString

    Future { Response(true, body) }
  }


  def add(req: Request): Future[Response] = { 
    update_group(req) {case (key, node) =>
      Tbl.add(key, node)
    }
  }




  def remove(req: Request): Future[Response] = {

    update_group(req) { case (key, node) =>
      Tbl.remove(key, node)
    }

  }



  def delete(req: Request): Future[Response] = {
    val key = Req.key(req)
    Tbl.delete(key)

    val gid = key.toString
    val rep_str = s"Group $gid was created"

    Future { Req.ok_rep(rep_str) }
  }



  def create(req: Request): Future[Response] = {
    val key = Req.key(req)
    Tbl.create(key)

    val gid = key.toString
    val rep_str = s"group $gid was created"
    Future { Req.ok_rep(rep_str) }
  }




  def list(req: Request): Future[Response] = {
    val payload =
      Req.key(req) |> Tbl.query |> NodeSet.toByteString

    val rep = Response(true, payload)

    Future.value(rep)
  }

  def list_groups() = {
    val keys = Tbl.S.iterator.toVector map {case (k, _) => k} 
    RMSG( KeyList.toByteArray(keys) ) 
  }

}

class Service(val Tbl: Table, val Srv: Server) {


  val Disseminator = Srv.D


  val Contents = new TableContents(Tbl)

  val TBLSync = new Sync(Contents, Srv.PS)
  val API = new Endpoints(Tbl)

  val RFilter = Srv.Rumor_Filters

  def dispatch(treq: TMSG)(cb: Request => Future[Response]) = {
    val req = Req.decode(treq)

    cb(req) map {r =>
      Req.encode(r) 
    }

  }


  def handle_call(flow: Flow[RMSG, TMSG], req: TMSG)(cb: Request => Future[Response]) = {

    RFilter.with_rumor(req) { case (tmsg, rumor) => 
      RFilter.first_responder(flow, tmsg, rumor)(req1 => dispatch(req1)(cb)  ) 
    }

  }





  def sync(flow: Flow[RMSG, TMSG], tmsg: TMSG) = {
    val data = Tbl.toMap
    TBLSync.rsync(flow, data, tmsg) map {x => ()}
  }



  def callback(flow: Flow[RMSG, TMSG], req: TMSG): Future[Unit] = {


    val handle = handle_call(flow, req)(_)

    req.path match {
      case List("pgroups", "create") => handle(API.create)
      case List("pgroups", "delete") => handle(API.delete)

      case List("pgroups", "add") => handle(API.add)
      case List("pgroups", "remove") => handle(API.remove)

      case List("pgroups", "list") =>

        dispatch(req)(
          API.list
        ) flatMap {rep => flow.write(rep) }

      case List("pgroups", "list_servers") =>
        Srv.view_memberlist flatMap {rep => flow.write(rep) }

      case List("pgroups", "sync") => sync(flow, req)

      case List("pgroups", "groups") =>
        API.list_groups() |> flow.write
    }

  }


  def do_sync() = {
    val path = Req.Path("pgroups/sync")
    val req = TMSG(path)

    if (Srv.Procs.notEmpty) { TBLSync.tsync(Srv.view, Tbl.toMap, req) }
  }


  
  def maintain(interval: Duration = Srv.Procs.defaultInterval) = {
     Srv.Procs.timer.schedule(interval)(do_sync)
  }

}

