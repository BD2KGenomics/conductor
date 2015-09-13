/*
Copyright 2015 UCSC Computational Genomics Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package edu.ucsc.cgl.conductor

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
  s3.createBucket(bucketName)

  val partSize = 1024 * 1024 * 8

  val s3Prefix = "s3://"
  val hdfsPrefix = "hdfs://"
  var dstPrefix = ""

  // Creates and uploads a 8 MB file
  val smallName = "simple_file"
  val smallFileSize = partSize
  var smallBytes: Array[Byte] = uploadFile(partSize, smallName)
  var smallSrc = ""
  var smallDst = ""
  var smallDownloader: Downloader = null

  // Creates and uploads a 20 MB file
  val bigName = "big_file"
  val bigFileSize = 1024 * 1024 * 20
  // A file to be divided into 2 full partitions and one half paritition
  var bigBytes: Array[Byte] = uploadFile(bigFileSize, bigName)
  var bigSrc = ""
  var bigDst = ""
  var bigDownloader: Downloader = null

  // Information to be needed later in the program for a directory of
  // differently-sized files
  val inconsistentName = "inconsistent_files"
  var inconsistentSrc: String = null
  var inconsistentDst: String = null

  // Creates an HDFS directory to store all downloaded files
  val hdfsDir = "/testDir/"
  var hdfs = new Path(dstPrefix + "/").getFileSystem(new Configuration())
  hdfs.mkdirs(new Path(dstPrefix + hdfsDir))

  // Information for the report for each test
  val downloadName = "Download Test:"
  val uploadDownloadName = "Upload/Download Test:"
  val partBlockSizeName = "Part/Block Size Test:"
  val inconsistentPartSizeName = "Inconsistent Part Size Test:"

  val downloadDescription = "Verifies that the downloaded bytes in each " +
    "partition of a download correspond to the correct part of the file."
  val uploadDownloadDescription = "Verifies that files can be uploaded and " +
    "downloaded with and without concatenation without changing the " +
    "content of the file."
  val partBlockSizeDescription = "Verifies that downloads have the same " +
    "results when using different part and block sizes."
  val inconsistentPartSizeDescription = "When valid, verifies that upload " +
    "and download work when parts are not the same size."

  var downloadSuccess = "NOT TESTED"
  var uploadDownloadSuccess = "NOT TESTED"
  var partBlockSizeSuccess = "NOT TESTED"
  var inconsistentPartSizeSuccess = "NOT TESTED"

  // Ends the SparkContext on the driver
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
        dstPrefix = hdfsPrefix + masterPublicDNS + ":/"

        // Generates the source and destination locations for different files
        // now that the public DNS is available
        smallSrc = s3Prefix + bucketName + "/" + smallName
        smallDst = dstPrefix + smallName
        smallDownloader =
          new Downloader(
            credentials,
            partSize,
            partSize,
            new URI(smallSrc),
            new URI(smallDst),
            true)

        bigSrc = s3Prefix + bucketName + "/" + bigName
        bigDst = dstPrefix + bigName
        bigDownloader =
          new Downloader(
            credentials,
            partSize,
            partSize,
            new URI(bigSrc),
            new URI(bigDst),
            true)

        inconsistentSrc = s3Prefix + bucketName + "/" + inconsistentName
        inconsistentDst = dstPrefix + hdfsDir + inconsistentName

        try {
          downloadTest()
          uploadDownloadTest()
          partBlockSizeTest()
          inconsistentPartSizeTest()
        } finally {
          stop()
          printReport()
        }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def stop() {
    // Removes all created files from S3 and deletes the bucket
    val summaries = s3.listObjects(bucketName).getObjectSummaries
    for (n <- 0 to summaries.size() - 1) {
      s3.deleteObject(bucketName, summaries.get(n).getKey)

    }
    s3.deleteBucket(bucketName)

    // Removes all files downloaded to HDFS
    hdfs = new Path(dstPrefix + "/").getFileSystem(new Configuration())
    hdfs.delete(new Path(dstPrefix + hdfsDir), true)
    hdfs.close()
  }

  // Generates a report for the end of the tests stating which tests passed
  def printReport(): Unit = {
    println("TEST REPORT")
    printTest(
      downloadName,
      downloadDescription,
      downloadSuccess)
    printTest(
      uploadDownloadName,
      uploadDownloadDescription,
      uploadDownloadSuccess)
    printTest(
      partBlockSizeName,
      partBlockSizeDescription,
      partBlockSizeSuccess)
    printTest(
      inconsistentPartSizeName,
      inconsistentPartSizeDescription,
      inconsistentPartSizeSuccess)
  }

  // Formats the report for each individual test
  def printTest(testName: String,
                testDescription: String,
                success: String): Unit = {
    println("")
    println(testName)
    println(testDescription)
    println(success)
  }

  // Creates, uploads to S3, and deletes a file of given name and size
  def uploadFile(size: Int, name: String): Array[Byte] = {
    val dst = name
    val bytes = new Array[Byte](size)
    val random = new Random()
    random.nextBytes(bytes)
    val file = new File(dst)
    val fos = new FileOutputStream(file)
    try {
      fos.write(bytes)
    } finally {
      fos.close()
    }
    s3.putObject(bucketName, name, new java.io.File(dst))
    file.delete()
    bytes

  }

  // Verifies that bytes downloaded by the "downloadPart" method correspond to
  // the bytes in the spaces of the original file denoted by the partitions
  def downloadTest() {
    try {
      val partitionsOne = smallDownloader.partition(partSize).toArray
      assert(smallDownloader.downloadPart(partitionsOne(0)).sameElements(smallBytes))

      val partitionsThree = bigDownloader.partition(bigFileSize).toArray
      assert(bigDownloader.downloadPart(partitionsThree(0))
        .sameElements(util.Arrays.copyOfRange(bigBytes, 0, partSize)))
      assert(bigDownloader.downloadPart(partitionsThree(1))
        .sameElements(util.Arrays.copyOfRange(bigBytes, partSize, 2 * partSize)))
      assert(bigDownloader.downloadPart(partitionsThree(2))
        .sameElements(util.Arrays.copyOfRange(bigBytes, 2 * partSize, bigFileSize)))

      downloadSuccess = "SUCCESS"
    } catch {
      case e: Exception =>
        e.printStackTrace()
        downloadSuccess = "ABORTED"
      case a: AssertionError =>
        a.printStackTrace()
        downloadSuccess = "FAILURE"
    } finally {
      // Resets all byte arrays to null to save memory
      bigBytes = null
      bigDownloader = null
      smallBytes = null
      smallDownloader = null
    }
  }

  // Uploads and downloads files and directories and verifies that the content
  // remains the same regardless of which type the content comes from and which
  // type it is assigned
  def uploadDownloadTest(): Unit = {
    try {
      // #1 download: bigfile -> file
      val dst1 = new URI(dstPrefix + hdfsDir + "load1")
      var load1 =
        new Downloader(
          credentials,
          partSize,
          partSize,
          new URI(bigSrc),
          dst1,
          true)
      load1.run()
      // Resets all byte arrays to null to save memory
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
      var load4 =
        new Downloader(credentials, partSize, partSize, dst2, dst4, true)
      load4.run()
      load4 = null

      // #5 download: #3 dir -> file
      val dst5 = new URI(dstPrefix + hdfsDir + "load5")
      var load5 =
        new Downloader(credentials, partSize, partSize, dst3, dst5, true)
      load5.run()
      load5 = null

      // #6 download: #3 dir -> dir
      val dst6 = new URI(dstPrefix + hdfsDir + "load6")
      var load6 =
        new Downloader(credentials, partSize, partSize, dst3, dst6, false)
      load6.run()
      load6 = null

      // #7 upload: #6 dir -> file
      val dst7 = new URI(s3Prefix + bucketName + "/load7")
      var load7 =
        new Uploader(credentials, dst6, dst7, true)
      load7.run()
      load7 = null

      // #8 download: #7 file -> file
      val dst8 = new URI(dstPrefix + hdfsDir + "load8")
      var load8 =
        new Downloader(credentials, partSize, partSize, dst7, dst8, true)
      load8.run()
      load8 = null

      // #9 download: #2 file -> dir
      val dst9 = new URI(dstPrefix + hdfsDir + "load9")
      var load9 =
        new Downloader(credentials, partSize, partSize, dst2, dst9, false)
      load9.run()
      load9 = null

      // #10 upload: #9 dir -> dir
      val dst10 = new URI(s3Prefix + bucketName + "/load10")
      var load10 = new Uploader(credentials, dst9, dst10, false)
      load10.run()
      load10 = null

      // #11 download: #10 dir -> file
      val dst11 = new URI(dstPrefix + hdfsDir + "load11")
      var load11 =
        new Downloader(credentials, partSize, partSize, dst10, dst11, true)
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

      uploadDownloadSuccess = "SUCCESS"
    } catch {
      case e: Exception =>
        e.printStackTrace()
        uploadDownloadSuccess = "ABORTED"
      case a: AssertionError =>
        a.printStackTrace()
        uploadDownloadSuccess = "FAILURE"
    }
  }

  // Tests that downloading files with different part sizes does not affect
  // content
  def partBlockSizeTest(): Unit = {
    try {
      // 8MB part, 8MB block
      val dst8_8 = new URI(dstPrefix + hdfsDir + "8_8")
      var load8_8 =
        new Downloader(
          credentials,
          partSize,
          partSize,
          new URI(bigSrc),
          dst8_8,
          true)
      load8_8.run()
      // Resets all byte arrays to null to save memory
      load8_8 = null

      // 8MB part, 4MB block
      val dst8_4 = new URI(dstPrefix + hdfsDir + "8_4")
      var load8_4 =
        new Downloader(
          credentials,
          partSize,
          partSize / 2,
          new URI(bigSrc),
          dst8_4,
          true)
      load8_4.run()
      load8_4 = null

      // 4MB part, 4MB block
      val dst4_4 = new URI(dstPrefix + hdfsDir + "4_4")
      var load4_4 =
        new Downloader(
          credentials,
          partSize / 2,
          partSize / 2,
          new URI(bigSrc),
          dst4_4,
          true)
      load4_4.run()
      load4_4 = null

      // 6MB part, 3MB block
      val dst6_3 = new URI(dstPrefix + hdfsDir + "6_3")
      var load6_3 =
        new Downloader(
          credentials,
          partSize / 2,
          partSize / 2,
          new URI(bigSrc),
          dst6_3,
          true)
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

      partBlockSizeSuccess = "SUCCESS"
    } catch {
      case e: Exception =>
        e.printStackTrace()
        partBlockSizeSuccess = "ABORTED"
      case a: AssertionError =>
        a.printStackTrace()
        partBlockSizeSuccess = "FAILURE"
    }

  }

  // Tests that a directory of files of different sizes can still be uploaded
  // and downloaded, except in downloading and concatenating
  def inconsistentPartSizeTest(): Unit = {

    try {

      val inconsistentSize1 = 1024 * 1024 * 5
      val inconsistentSize2 = 1024 * 1024 * 6
      val inconsistentSize3 = 1024 * 1024 * 7
      var inconsistentBytes1 =
        uploadFile(inconsistentSize1, inconsistentName + "_1")
      var inconsistentBytes2 =
        uploadFile(inconsistentSize2, inconsistentName + "_2")
      var inconsistentBytes3 =
        uploadFile(inconsistentSize3, inconsistentName + "_3")

      // Moves the recently-uploaded objects into a "directory" on S3 and
      // deletes the original copies
      s3.copyObject(
        bucketName,
        inconsistentName + "_1",
        bucketName,
        inconsistentName + "/1")
      s3.deleteObject(bucketName, inconsistentName + "_1")
      s3.copyObject(
        bucketName,
        inconsistentName + "_2",
        bucketName,
        inconsistentName + "/2")
      s3.deleteObject(bucketName, inconsistentName + "_2")
      s3.copyObject(
        bucketName,
        inconsistentName + "_3",
        bucketName,
        inconsistentName + "/3")
      s3.deleteObject(bucketName, inconsistentName + "_3")

      var inconsistentConcatDownloader =
        new Downloader(
          credentials,
          partSize,
          partSize,
          new URI(inconsistentSrc),
          new URI(inconsistentDst),
          true)
      // Running a download where parts are concatenated should fail because
      // because the parts have different sizes
      try {
        inconsistentConcatDownloader.run()
        assert(false, "Running should have failed because the parts are " +
          "different sizes and cannot be concatenated.")
      } catch {
        case _: AssertionError =>
      } finally {
        inconsistentConcatDownloader = null
      }

      var inconsistentNoConcatDownloader =
        new Downloader(
          credentials,
          partSize,
          partSize,
          new URI(inconsistentSrc),
          new URI(inconsistentDst),
          false)
      inconsistentNoConcatDownloader.run()
      val hdfs2 = new Path(dstPrefix + "/").getFileSystem(new Configuration())
      // Checks that the individual files of a non-concatenated download equal
      // the original files generated
      assert(util.Arrays.equals(inconsistentBytes1,
        IOUtils.toByteArray(hdfs2.open(new Path(inconsistentDst + "/1")))))
      assert(util.Arrays.equals(inconsistentBytes2,
        IOUtils.toByteArray(hdfs2.open(new Path(inconsistentDst + "/2")))))
      assert(util.Arrays.equals(inconsistentBytes3,
        IOUtils.toByteArray(hdfs2.open(new Path(inconsistentDst + "/3")))))
      inconsistentNoConcatDownloader = null

      val concatUploadName = "inconsistentConcat"
      var inconsistentConcatUploader =
        new Uploader(credentials,
          new URI(inconsistentDst),
          new URI(s3Prefix + bucketName + "/" + concatUploadName),
          true)
      inconsistentConcatUploader.run()
      // Checks that
      assert(util.Arrays.equals(
        IOUtils.toByteArray(
          s3.getObject(bucketName, concatUploadName).getObjectContent),
        inconsistentBytes1 ++ inconsistentBytes2 ++ inconsistentBytes3))
      inconsistentConcatUploader = null

      val uploadName = "inconsistent"
      var inconsistentNoConcatUploader =
        new Uploader(credentials,
          new URI(inconsistentDst),
          new URI(s3Prefix + bucketName + "/" + uploadName),
          false)
      inconsistentNoConcatUploader.run()
      // Checks that the individual files of a non-concatenated upload equal
      // the original files generated
      assert(util.Arrays.equals(
        IOUtils.toByteArray(
          s3.getObject(bucketName, uploadName + "/part-00001")
            .getObjectContent),
        inconsistentBytes1))
      assert(util.Arrays.equals(
        IOUtils.toByteArray(
          s3.getObject(bucketName, uploadName + "/part-00002")
            .getObjectContent),
        inconsistentBytes2))
      assert(util.Arrays.equals(
        IOUtils.toByteArray(
          s3.getObject(bucketName, uploadName + "/part-00003")
            .getObjectContent),
        inconsistentBytes3))

      // Resets all byte arrays to null to save memory
      inconsistentNoConcatUploader = null
      inconsistentBytes1 = null
      inconsistentBytes2 = null
      inconsistentBytes3 = null

      inconsistentPartSizeSuccess = "SUCCESS"
    } catch {
      case e: Exception =>
        e.printStackTrace()
        inconsistentPartSizeSuccess = "ABORTED"
      case a: AssertionError =>
        a.printStackTrace()
        inconsistentPartSizeSuccess = "FAILURE"
    }
  }

}

