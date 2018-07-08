package PGroups

import org.scalatest._
import Enkidu._
import Loadbalancer.{ServerList}
import Mux.{TMSG,RMSG, Path}

import io.netty.channel.socket.nio.{NioSocketChannel,  NioServerSocketChannel}
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption}

import com.twitter.util.{Future, Await}
import scala.sys.process._

import Datastore.Key

object Helpers {

  val pool = WorkerPool.default 

  val clientInitializer = new ChannelInitializer[Channel] {
    def initChannel(c: Channel) = {
      Mux.Pipelines.clientPipeline(c.pipeline)
    }
  }


  def makeCM()  = {

    val bootstrap = Connection.bootstrap(
      classOf[NioSocketChannel],
      clientInitializer,
      pool
    )

    EndpointPool[TMSG, RMSG](bootstrap)

  }



}


class EndpointsSuite extends FunSuite {

  val local = "localhost"


  def makeNodes(start: Int, i: Int) = {
    val max = start + (i - 1)
    (start to max) map {x => Node(local, x) } toSet
  }


  val nodes = new ServerList( makeNodes(3000, 5) )
  val CM = Helpers.makeCM() 

  val PG = new Client(nodes, CM)


  val srv = "cluster-test/run.sh".run
  Thread.sleep(2000)


  test("Runs updates") {


    val n1 = makeNodes(6000, 8).toList.sortWith(_.port < _.port)

    val f = Future.collect {
      n1.toVector.map {x =>
        PG.add(
          Key("/file_servers/a"),
          x
        )
      }
   
    }


    Await.result(f)

    Thread.sleep(300)

    val res = Await.result(
      PG.list( Key("/file_servers/a") )
    )


    val got = res.toList.sortWith(_.port < _.port)
    assert(got == n1)

    val rm_f = PG.remove( Key("/file_servers/a"), n1.last )

    Await.result(rm_f)



    Thread.sleep(300)


    val got1 = Await.result(
      PG.list(Key( "file_servers/a") ) 
    )



    assert(
      got1.contains(n1.last) == false
    )


  }



  test("Manage Groups") {


    val key_b = Key("/file_servers/b")
    Await.result( PG.create(key_b) )


    val list = Await.result( PG.groups() )


    Await.result( PG.delete(key_b) )


    Thread.sleep(350)

    val got1 = Await.result( PG.groups() )
    assert(got1.contains(key_b) == false )


  }


  srv.destroy


}
