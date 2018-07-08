import Legion._
import PGroups._ 
import Enkidu._
import Legion.PeerService.{Server}

import Mux.{TMSG, RMSG}
import TestNet._
import Formation.RangedLocalHost

import Datastore.MapStore


object CMap extends RangedLocalHost(3000) {

  def makeTable(id: String) = {
    val db = new MapStore[MemberList.T]()
    new Table(id, db)
  }


  def make(S: Server) = {
    val T = makeTable(S.view.local.toString)
    val Svc = new Service(T, S)

    Svc.maintain()

    new Handler {
      def callback(flow: Flow[RMSG, TMSG], req: TMSG) = Svc.callback(flow, req)
    }

  }


}


object Cluster extends App {
  new TestNet.Cluster(5, CMap)
}
