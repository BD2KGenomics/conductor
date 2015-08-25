package edu.ucsc.bd2k

import java.io.{File, FileOutputStream}
import java.net.URI
import java.util
import java.util.Random

import com.amazonaws.services.s3.AmazonS3Client
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}

case class TestConfig(masterPublicDNS: String = "")

object ConductorIntegrationTests {

  val credentials = Credentials()

  val s3 = new AmazonS3Client(credentials.toAwsCredentials)
  val conf = new SparkConf().setAppName("spark-s3-downloader Tests")
  val sc = new SparkContext(conf)
  val bucketName = "s3-downloader-tests"
  //val partSize = 64 * 1024 * 1024
  val partSize = 1024 * 1024 * 8
  s3.createBucket(bucketName)

  val s3Prefix = "s3://"
  val hdfsPrefix = "hdfs://"
  var dstPrefix = ""

  val smallName = "simple_file"
  val smallFileSize = partSize
  var smallBytes: Array[Byte] = uploadFile(partSize, smallName)
  val smallSrc = s3Prefix + bucketName + "/" + smallName
  val smallDst = dstPrefix + smallName
  var smallDownloader =
    new Downloader(
      credentials,
      partSize,
      partSize,
      new URI(smallSrc),
      new URI(smallDst),
      true)

  val big = "big_file"
  //val bigFileSize = 64 * 1024 * 1024 * 5 / 2
  val bigFileSize = 1024 * 1024 * 5 / 2 * 8
  // A file to be divided into 2 full partitions and one half paritition
  var bigBytes: Array[Byte] = uploadFile(bigFileSize, big)
  val bigSrc = s3Prefix + bucketName + "/" + big
  val bigDst = dstPrefix + big
  var bigDownloader =
    new Downloader(
      credentials,
      partSize,
      partSize,
      new URI(bigSrc),
      new URI(bigDst),
      true)

  val hdfsDir = "/testDir/"
  var hdfs = new Path(dstPrefix + "/").getFileSystem(new Configuration())
  //val hdfs3 = new Path(hdfsPrefix + "/").getFileSystem(new Configuration())
  hdfs.mkdirs(new Path(dstPrefix + hdfsDir))

  sc.stop()

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[TestConfig]("scopt") {
      arg[String]("master-public-dns") action { (x, c) =>
        c.copy(masterPublicDNS = x) } text("the public DNS corresponding to" +
        " the master of the cluster where files will be downloaded to and" +
        " uploaded from")
    }
    parser.parse(args, TestConfig()) match {
      case Some(config) =>
        val masterPublicDNS = config.masterPublicDNS
        dstPrefix = hdfsPrefix + masterPublicDNS
        try {
          downloadTest()
          uploadDownloadTest()
          partBlockSizeTest()

        } finally {
          stop()
        }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def stop() {
    val summaries = s3.listObjects(bucketName).getObjectSummaries
    for (n <- 0 to summaries.size() - 1) {
      s3.deleteObject(bucketName, summaries.get(n).getKey)

    }
    s3.deleteBucket(bucketName)
    hdfs = new Path(dstPrefix + "/").getFileSystem(new Configuration())
    hdfs.delete(new Path(dstPrefix + hdfsDir), true)
    hdfs.close()
  }


  def uploadFile(size: Int, name: String): Array[Byte] = {
    val dst = name
    val bytes = new Array[Byte](size)
    val random = new Random()
    random.nextBytes(bytes)
    val fos = new FileOutputStream(new File(dst))
    try {
      fos.write(bytes)
    } finally {
      fos.close()
    }
    s3.putObject(bucketName, name, new java.io.File(dst))
    bytes
  }

  def downloadTest() {
    val partitionsOne = smallDownloader.partition(partSize).toArray
    assert(smallDownloader.downloadPart(partitionsOne(0)).sameElements(smallBytes))

    val partitionsThree = bigDownloader.partition(bigFileSize).toArray
    assert(bigDownloader.downloadPart(partitionsThree(0))
      .sameElements(util.Arrays.copyOfRange(bigBytes, 0, partSize)))
    assert(bigDownloader.downloadPart(partitionsThree(1))
      .sameElements(util.Arrays.copyOfRange(bigBytes, partSize, 2 * partSize)))
    assert(bigDownloader.downloadPart(partitionsThree(2))
      .sameElements(util.Arrays.copyOfRange(bigBytes, 2 * partSize, bigFileSize)))

    bigBytes = null
    bigDownloader = null
    smallBytes = null
    smallDownloader = null
  }

  def uploadDownloadTest(): Unit = {

    // #1 download: bigfile -> file
    val dst1 = new URI(dstPrefix + hdfsDir + "load1")
    var load1 = new Downloader(credentials, partSize, partSize, new URI(bigSrc), dst1, true)
    load1.run()
    load1 = null

    // #2 upload: #1 file -> file
    val dst2 = new URI(s3Prefix + bucketName + "/load2")
    var load2 = new Uploader(credentials, dst1, dst2, true)
    load2.run()
    load2 = null

    // #3 upload: #1 file -> dir
    val dst3 = new URI(s3Prefix + bucketName + "/load3")
    var load3 = new Uploader(credentials, dst1, dst3, false)
    load3.run()
    load3 = null

    // #4 download: #2 file -> file
    val dst4 = new URI(dstPrefix + hdfsDir + "load4")
    var load4 = new Downloader(credentials, partSize, partSize, dst2, dst4, true)
    load4.run()
    load4 = null

    // #5 download: #3 dir -> file
    val dst5 = new URI(dstPrefix + hdfsDir + "load5")
    var load5 = new Downloader(credentials, partSize, partSize, dst3, dst5, true)
    load5.run()
    load5 = null

    // #6 download: #3 dir -> dir
    val dst6 = new URI(dstPrefix + hdfsDir + "load6")
    var load6 = new Downloader(credentials, partSize, partSize, dst3, dst6, false)
    load6.run()
    load6 = null

    // #7 upload: #6 dir -> file
    val dst7 = new URI(s3Prefix + bucketName + "/load7")
    var load7 = new Uploader(credentials, dst6, dst7, true)
    load7.run()
    load7 = null

    // #8 download: #7 file -> file
    val dst8 = new URI(dstPrefix + hdfsDir + "load8")
    var load8 = new Downloader(credentials, partSize, partSize, dst7, dst8, true)
    load8.run()
    load8 = null

    // #9 download: #2 file -> dir
    val dst9 = new URI(dstPrefix + hdfsDir + "load9")
    var load9 = new Downloader(credentials, partSize, partSize, dst2, dst9, false)
    load9.run()
    load9 = null

    // #10 upload: #9 dir -> dir
    val dst10 = new URI(s3Prefix + bucketName + "/load10")
    var load10 = new Uploader(credentials, dst9, dst10, false)
    load10.run()
    load10 = null

    // #11 download: #10 dir -> file
    val dst11 = new URI(dstPrefix + hdfsDir + "load11")
    var load11 = new Downloader(credentials, partSize, partSize, dst10, dst11, true)
    load11.run()
    load11 = null

    val hdfs2 = new Path(dstPrefix + "/").getFileSystem(new Configuration())
    var bytes1 = IOUtils.toByteArray(hdfs2.open(new Path(dst1)))

    // #4 == #1 (upload: file -> file, download: file -> file)
    var bytes4 = IOUtils.toByteArray(hdfs2.open(new Path(dst4)))
    assert(util.Arrays.equals(bytes1, bytes4))
    bytes4 = null

    // #5 == #1 (upload: file -> dir, download: dir -> file)
    var bytes5 = IOUtils.toByteArray(hdfs2.open(new Path(dst5)))
    assert(util.Arrays.equals(bytes1, bytes5))
    bytes5 = null

    // #8 == #1 (upload: dir -> file, download: dir -> dir)
    var bytes8 = IOUtils.toByteArray(hdfs2.open(new Path(dst8)))
    assert(util.Arrays.equals(bytes1, bytes8))
    bytes8 = null

    // #11 == #1 (upload: dir -> dir, download: file -> dir)
    var bytes11 = IOUtils.toByteArray(hdfs2.open(new Path(dst11)))
    assert(util.Arrays.equals(bytes1, bytes11))
    bytes11 = null
    bytes1 = null

    hdfs2.close()

  }

  def partBlockSizeTest(): Unit = {
    // 8MB part, 8MB block
    val dst8_8 = new URI(dstPrefix + hdfsDir + "8_8")
    var load8_8 = new Downloader(credentials, partSize, partSize, new URI(bigSrc), dst8_8, true)
    load8_8.run()
    load8_8 = null

    // 8MB part, 4MB block
    val dst8_4 = new URI(dstPrefix + hdfsDir + "8_4")
    var load8_4 = new Downloader(credentials, partSize, partSize / 2, new URI(bigSrc), dst8_4, true)
    load8_4.run()
    load8_4 = null

    // 4MB part, 4MB block
    val dst4_4 = new URI(dstPrefix + hdfsDir + "4_4")
    var load4_4 = new Downloader(credentials, partSize / 2, partSize / 2, new URI(bigSrc), dst4_4, true)
    load4_4.run()
    load4_4 = null

    // 6MB part, 3MB block
    val dst6_3 = new URI(dstPrefix + hdfsDir + "6_3")
    var load6_3 = new Downloader(credentials, partSize / 2, partSize / 2, new URI(bigSrc), dst6_3, true)
    load6_3.run()
    load6_3 = null

    val hdfs2 = new Path(dstPrefix + "/").getFileSystem(new Configuration())
    var bytes8_8 = IOUtils.toByteArray(hdfs2.open(new Path(dst8_8)))

    var bytes8_4 = IOUtils.toByteArray(hdfs2.open(new Path(dst8_4)))
    assert(util.Arrays.equals(bytes8_8, bytes8_4))
    bytes8_4 = null

    var bytes4_4 = IOUtils.toByteArray(hdfs2.open(new Path(dst4_4)))
    assert(util.Arrays.equals(bytes8_8, bytes4_4))
    bytes4_4 = null

    var bytes6_3 = IOUtils.toByteArray(hdfs2.open(new Path(dst6_3)))
    assert(util.Arrays.equals(bytes8_8, bytes6_3))
    bytes6_3 = null

    bytes8_8 = null

  }
}

