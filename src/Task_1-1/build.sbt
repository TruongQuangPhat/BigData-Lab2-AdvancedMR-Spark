name := "SlidingWindowJob"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.3.1" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.1" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.1" % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
