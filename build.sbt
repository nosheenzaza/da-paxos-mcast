name := "da-paxos"

version := "1.0"

scalaVersion := "2.11.6"

lazy val akkaVersion = "2.4.0"

lazy val scoptVersion = "3.3.0"

resolvers += Resolver.sonatypeRepo("public")

// retrieveManaged := true

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.github.scopt" %% "scopt" % scoptVersion
)

fork in run := true