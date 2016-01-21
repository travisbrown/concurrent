scalaVersion := "2.11.7"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.spire-math" %% "cats-core" % "0.4.0-SNAPSHOT"
)
