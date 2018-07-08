package PGroups
import org.scalatest._
import Enki.PipeOps._
import Enkidu.Node


class CodecSuite extends FunSuite {



  test("Node Codec") {

    val expect = Node(80)
    val got_s = NodeCodec.toString(expect) |> NodeCodec.fromString

    assert(expect == got_s)

    val got_bs = NodeCodec.toByteString(expect) |> NodeCodec.fromByteString
    assert(expect == got_bs)

    val got_bytes = NodeCodec.toByteArray(expect) |> NodeCodec.fromByteArray
    assert(expect == got_bytes)
  }



  test("Node Set") {
    val nodes = Set(Node(1000), Node(3000), Node(5000) )

    val got = NodeSet.toByteString(nodes) |> NodeSet.fromByteString
    assert(nodes == got)

  }

  

}
