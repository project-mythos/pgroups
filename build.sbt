


lazy val commonSettings = Seq(
  organization := "demons",
  version := "0.3-SNAPSHOT",

  libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.4"
)


lazy val pg_deps = Seq(
  "demons" %% "legion" % "0.3-SNAPSHOT",
  "demons" %% "crdt-store" % "1.0-SNAPSHOT", 
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)





lazy val pgroups =
  ( project in file(".") ).settings(
    commonSettings,
    name := "pgroups",
    libraryDependencies ++= pg_deps,


    PB.targets in Compile := Seq(
      scalapb.gen(grpc=false) -> (sourceManaged in Compile).value
    )
      
  )



lazy val test_deps = Seq(
  "demons" %% "legion-testnet" % "0.3-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

lazy val cluster_test = ( project in file("cluster-test") )
  .dependsOn(pgroups)
  .settings(
    commonSettings,
    libraryDependencies ++= test_deps,
    assemblyJarName in assembly := "cluster-test.jar",
    test in assembly := {}
  )





def test_cluster() = Command.command("test-cluster") { state =>
  "project cluster_test" ::
  "assembly" ::
  "test" ::
  "project pgroups" :: 
  state
}


commands += test_cluster
