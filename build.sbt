name := "scaldinghistogram"

version := "1.0"

scalaVersion := "2.11.6"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

resolvers += "repo.codehale.com" at "http://repo.codahale.com"

libraryDependencies ++= Seq(
  "com.twitter" % "scalding-core_2.11" % "0.13.1",
  "com.twitter" % "algebird-core_2.11" % "0.9.0",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1"
)

scalacOptions ++= Seq(
  "-feature"
  //  "-Ylog-classpath"
)