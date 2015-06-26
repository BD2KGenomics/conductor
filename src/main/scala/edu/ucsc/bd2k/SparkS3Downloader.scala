package edu.ucsc.bd2k

import java.net.URI

import com.amazonaws.AmazonClientException
import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, Reporter}
import org.apache.hadoop.util.Progressable
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object SparkS3Downloader {

  def main(args: Array[String]) {   
    
    val credentials = Credentials()
    val parser = new scopt.OptionParser[Config]("scopt") {
      opt[Int]("s3-part-size") action { (x, c) =>
        c.copy(s3PartSize = x)} text("s3-part-size indicates the size of each partition in MB; default: 64")
      opt[Int]("hdfs-block-size") action { (x, c) => 
        c.copy(hdfsBlockSize = x)} text("hdfs-block-size indicates the size of each block in MB; must divide s3-part-size evenly; default: 64")
      arg[String]("src-path") action { (x, c) =>
        c.copy(srcLocation = x) } text("location of src file")
      arg[String]("dst-path") action { (x, c) =>
        c.copy(dstLocation = x) } text("location of dst file")
    }
    parser.parse(args, Config()) match {
      case Some(config) =>
        val partitionSize = config.s3PartSize * 1024 * 1024
        val blockSize = config.hdfsBlockSize * 1024 * 1024
        assert(partitionSize % blockSize == 0, "Partition size must be a multiple of block size.")
        val src = new URI(config.srcLocation)
        assert(src.getScheme == "s3", "The source file must be in S3.")
        val dst = new URI(config.dstLocation)
        assert(dst.getScheme == "hdfs", "The destination location must be in HDFS (Hadoop Distributed File System).")
        new SparkS3Downloader(credentials, partitionSize, blockSize, src, dst).run()
      case None =>
        // arguments are bad, error message will have been displayed
    }
  }
}

case class Config(s3PartSize: Int = 64, hdfsBlockSize: Int = 64, srcLocation: String = "", dstLocation: String = "")

class SparkS3Downloader(credentials: Credentials, partitionSize: Int, blockSize: Int, src: URI, dst: URI) extends Serializable {

  val srcBucket = src.getHost
  val srcPath = src.getPath
  assert(srcPath(0) == '/', "The path to the source file must be valid and start with '/'.")
  val srcKey = srcPath.substring(1)
  val splitDst = new URI(dst.toString + ".parts")
  val blockSizeConf = "sparkS3Downloader.blockSize"

  def run() {
    val s3 = new AmazonS3Client(credentials.toAwsCredentials)
    val size: Long = s3.getObjectMetadata(srcBucket, srcKey).getContentLength
    val partitions: ArrayBuffer[(Long, Int)] = partition(size)
    val conf = new SparkConf().setAppName("SparkS3Downloader")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.setInt(blockSizeConf, blockSize)
    sc.parallelize(partitions, partitions.size)
      .map(partition => (partition, download(partition._1, partition._2)))
      .saveAsHadoopFile(splitDst.toString, classOf[Object], classOf[Array[Byte]], classOf[BinaryOutputFormat[Object]])
    concat(sc, splitDst, dst)
  }

  def partition(size: Long): ArrayBuffer[(Long, Int)] = {
    val numPartitions = size / partitionSize
    val remainder = (size % partitionSize).toInt
    val partitions = new ArrayBuffer[(Long, Int)]
    for (i <- 0L until numPartitions) {
      partitions += ((i * partitionSize, partitionSize))
    }
    if (remainder > 0) {
      partitions += ((size - remainder, remainder))
    }
    partitions
  }

  def download(start: Long, size: Int): Array[Byte] = {
    assert(size > 0, "Partitions cannot be empty.")
    assert(size <= partitionSize, "No partition can exceed the specified part size.")
    val s3 = new AmazonS3Client(credentials.toAwsCredentials)
    val req = new GetObjectRequest(srcBucket, srcKey)
    req.setRange(start, start + size - 1)
    val in = s3.getObject(req).getObjectContent
    try {
      /*
       * Knowing the input size is obviously an advantage over blindly copying
       * from a stream of unknown size. IOW, I expect the result array to be
       * allocated once. Currently I can only find this in commons.io. Guava
       * has it, too, but doesn't expose it publicly. Also, anything based on
       * ByteArrayOutputStream will likely incur one final redundant copy when
       * the ByteArrayOutputStream.toByteArray method is called. scalax.io
       * calls Thread.sleep() in its copy methods which leads to immediate
       * disqualification. But it also doesn't allow specifying a known size
       * from what I can tell.
       */
      IOUtils.toByteArray(in, size)
    } finally {
      in.close()
    }
  }

  def concat(sc: SparkContext, splitDst: URI, dst: URI): Unit = {
    val dfs = FileSystem.get(dst, sc.hadoopConfiguration).asInstanceOf[DistributedFileSystem]
    val splitDstPath = new Path(splitDst)
    val dstPath = new Path(dst)
    val parts = dfs.listStatus(splitDstPath, new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith("part-")
    }).map(_.getPath).sortBy(_.getName)
    val Array(firstPart, otherParts @ _*) = parts
    if (otherParts.nonEmpty) {
      dfs.concat(firstPart, otherParts.toArray)
    }
    dfs.rename(firstPart, dstPath)
    dfs.delete(splitDstPath, true)
  }
}


class BinaryOutputFormat[K] extends FileOutputFormat[K, Array[Byte]] {

  val blockSizeConf = "sparkS3Downloader.blockSize"

  override def getRecordWriter(ignored: FileSystem,
                               job: JobConf,
                               name: String,
                               progress: Progressable): mapred.RecordWriter[K, Array[Byte]] = {
    val file: Path = FileOutputFormat.getTaskOutputPath(job, name)
    val fs: FileSystem = file.getFileSystem(job)
    /*
     * Concatenation only works if all but the last part use an integral
     * number of blocks. We ensure this by setting the block size for this
     * "file" (quotes because it really is a directory) to the partition
     * size. That way, each part except the last one will take up one block.
     */
    // FIXME: This call duplicates too much internal logic from FileSystem
    val fileOut: FSDataOutputStream = fs.create(
      file,
      true,
      fs.getConf.getInt("io.file.buffer.size", 4096),
      fs.getDefaultReplication(file),
      job.getInt(blockSizeConf, 0),
      progress)
    new mapred.RecordWriter[K, Array[Byte]] {
      override def write(key: K, value: Array[Byte]): Unit = {
        fileOut.write(value)
      }

      override def close(reporter: Reporter): Unit = {
        fileOut.close()
      }
    }
  }
}


abstract class Credentials {
  def toAwsCredentials: AWSCredentials
}


object Credentials {
  def apply() = {
    // If we can get configured credentials on the driver, use those on every
    // worker node. Otherwise fall back to instance profile credentials which
    // will be read from instance metadata on each node. Note that even if the
    // driver node has access to instance profile credentials, we wouldn't be
    // able to use them on worker nodes.
    try {
      new ExplicitCredentials(new ConfiguredCredentials().toAwsCredentials)
    } catch {
      case _: AmazonClientException => new ImplicitCredentials
    }
  }
}


@SerialVersionUID(0L)
case class ExplicitCredentials(accessKeyId: String, secretKey: String)
  extends Credentials with Serializable {

  def this(awsCredentials: AWSCredentials) {
    this(awsCredentials.getAWSAccessKeyId, awsCredentials.getAWSSecretKey)
  }

  def toAwsCredentials: AWSCredentials = {
    new BasicAWSCredentials(accessKeyId, secretKey)
  }
}


@SerialVersionUID(0L)
case class ConfiguredCredentials()
  extends Credentials with Serializable {

  def toAwsCredentials: AWSCredentials = {
    new AWSCredentialsProviderChain(
      new EnvironmentVariableCredentialsProvider,
      new SystemPropertiesCredentialsProvider,
      new ProfileCredentialsProvider
    ).getCredentials
  }
}


@SerialVersionUID(0L)
case class ImplicitCredentials()
  extends Credentials with Serializable {

  def toAwsCredentials: AWSCredentials = {
    new DefaultAWSCredentialsProviderChain().getCredentials
  }
}
