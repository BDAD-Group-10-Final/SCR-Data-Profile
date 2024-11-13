package edu.nyu.yx3494
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  // spark session
  val spark = SparkSession.builder().appName("SCR Data Profile").getOrCreate()

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SCR <root folder of SCR data>");
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SCR Data Profile").set("spark.ui.port", "4141")
    val sc = new SparkContext(conf)

    val rootFolder = args(0)
    // get all files in the root folder
    val files = sc.wholeTextFiles(rootFolder)
    // filter out the files that are not .mdb files
    val mdbFiles = files.filter { case (path, _) => path.endsWith(".mdb") }
    // get the file names
    System.out.println("MDB files found(" + mdbFiles.count() + "):")
    mdbFiles.collect().foreach { case (path, _) => System.out.println(path) }

    // read the .mdb files
    mdbFiles.foreach { case (path, _) => readMDBFile(path) }

    sc.stop()
  }

  private def readMDBFile(path: String): Unit = {
    // spark sql
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:ucanaccess://" + path)
      .option("dbtable", "table1")
      .load()

    System.out.println("Schema of " + path + ":")

    df.printSchema()
  }
}