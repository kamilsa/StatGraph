name := "StatGraph"

version := "1.0"

scalaVersion := "2.11.7"

unmanagedJars in Compile += file("/usr/local/fproject/lib/spark-assembly-1.6.2-hadoop2.4.0.jar")

libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"