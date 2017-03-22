name := "sqs-test"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies += "com.amazonaws" % "aws-java-sdk-sqs" % "1.9.19"

fork in run := true
