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

import java.io.ByteArrayInputStream
import java.net.URI
import java.util
import java.util.{Random, UUID}

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.scalatest.{BeforeAndAfter, FunSuite, OneInstancePerTest}

import scala.collection.JavaConverters._
import scala.math.min

class ConductorIntegrationTests extends FunSuite with OneInstancePerTest with BeforeAndAfter
{
    val credentials = Credentials( )
    val testDirName = "conductor-tests-" + UUID.randomUUID( )

    val partSize = 1024 * 1024 * 8

    val s3Prefix = "s3://" + testDirName + "/"
    val hdfsPrefix = "hdfs://spark-master/" + testDirName + "/"

    val smallName = "small_file"
    val bigName = "big_file"
    val bigFileSize = 2 * partSize + partSize / 2

    val bigSrc = new URI( s3Prefix + bigName )
    val bigDst = new URI( hdfsPrefix + bigName )

    val smallBytes: Array[Byte] = randomBytes( partSize )
    val bigBytes: Array[Byte] = randomBytes( bigFileSize )

    val s3 = new AmazonS3Client( credentials.toAwsCredentials )
    s3.setRegion( Region.getRegion( Regions.US_WEST_2 ) )

    before {
        s3.createBucket( testDirName )
        val hdfs = new Path( hdfsPrefix + "/" ).getFileSystem( new Configuration( ) )
        try {
            hdfs.mkdirs( new Path( hdfsPrefix ) )
            uploadFile( smallName, smallBytes )
            uploadFile( bigName, bigBytes )
        } finally {
            hdfs.close( )
        }
    }

    after {
        // Clean up S3
        val os = s3.listObjects( testDirName ).getObjectSummaries.asScala
        os.foreach( o => s3.deleteObject( testDirName, o.getKey ) )
        s3.deleteBucket( testDirName )

        // Clean up HDFS
        val hdfs = new Path( hdfsPrefix + "/" ).getFileSystem( new Configuration( ) )
        try {
            hdfs.delete( new Path( hdfsPrefix ), true )
        } finally {
            hdfs.close( )
        }
    }

    // Verifies that bytes downloaded by the "downloadPart" method correspond to
    // the bytes in the spaces of the original file denoted by the partitions

    test( "downloadTest" ) {
        val bigDownloader = download( bigSrc, bigDst, concat = true )
        val smallSrc = s3Prefix + smallName
        val smallDst = hdfsPrefix + smallName
        val smallDownloader = download( new URI( smallSrc ), new URI( smallDst ), concat = true )
        val partitionsOne = smallDownloader.partition( partSize )
        assert( partitionsOne.length == 1 )
        val actual: Array[Byte] = smallDownloader.downloadPart( partitionsOne.head )
        assertByteArrayEquals( actual, smallBytes )
        val partitionsThree = bigDownloader.partition( bigFileSize )
        assert( partitionsThree.length == 3 )
        for( i <- 0 to 2 ) {
            val start: Int = i * partSize
            val end: Int = min( start + partSize, bigBytes.length )
            val actual = bigDownloader.downloadPart( partitionsThree( i ) )
            val expected = util.Arrays.copyOfRange( bigBytes, start, end )
            assertByteArrayEquals( actual, expected )
        }
    }

    // Uploads and downloads files and directories and verifies that the content
    // remains the same regardless of which type the content comes from and which
    // type it is assigned

    test( "uploadDownloadTest" ) {
        val s3file0: URI = bigSrc

        val hdfsFile1 = new URI( hdfsPrefix + "load1" )
        download( s3file0, hdfsFile1, concat = true ).run()
        assertFile( hdfsFile1, bigBytes )

        val s3File2 = new URI( s3Prefix + "load2" )
        upload( hdfsFile1, s3File2, concat = true ).run()

        val s3Dir3 = new URI( s3Prefix + "load3" )
        upload( hdfsFile1, s3Dir3, concat = false ).run()

        val hdfsFile4 = new URI( hdfsPrefix + "load4" )
        download( s3File2, hdfsFile4, concat = true ).run()
        assertFile( hdfsFile4, bigBytes )

        val hdfsFile5 = new URI( hdfsPrefix + "load5" )
        download( s3Dir3, hdfsFile5, concat = true ).run()
        assertFile( hdfsFile5, bigBytes )

        val hdfsDir6 = new URI( hdfsPrefix + "load6" )
        download( s3Dir3, hdfsDir6, concat = false ).run()

        val s3File7 = new URI( s3Prefix + "load7" )
        upload( hdfsDir6, s3File7, concat = true ).run()

        val dst8 = new URI( hdfsPrefix + "load8" )
        download( s3File7, dst8, concat = true ).run()
        assertFile( dst8, bigBytes )

        val dst9 = new URI( hdfsPrefix + "load9" )
        download( s3File2, dst9, concat = false ).run()

        val dst10 = new URI( s3Prefix + "load10" )
        upload( dst9, dst10, concat = false ).run()

        val dst11 = new URI( hdfsPrefix + "load11" )
        download( dst10, dst11, concat = true ).run()
        assertFile( dst11, bigBytes )
    }

    // Tests that downloading files with different part sizes does not affect content
    test( "partBlockSizeTest" ) {
        val n: Int = partSize
        List( (n, n), (n, n / 2), (n / 2, n / 2), (n * 3 / 4, n * 3 / 8) ).foreach {
            case (s3partSize, hdfsBlockSize) =>
                val dst = new URI( s"$hdfsPrefix-$s3partSize-$hdfsBlockSize" )
                download( bigSrc, dst, concat = true ).run( )
                assertFile( dst, bigBytes )
        }
    }

    // Tests that a directory of files of different sizes can still be uploaded
    // and downloaded, except when downloading with concat
    test( "partSizeValidationTest" ) {
        val prefix: String = "partSizeValidation"

        val originalName = prefix + "_originals"
        val s3Original: URI = new URI( s3Prefix + originalName )
        val hdfsOriginal: URI = new URI( hdfsPrefix + originalName )

        // Upload three random byte arrays
        val bufs = for( i <- 0 to 2 ) yield {
            val buf: Array[Byte] = randomBytes( (5 + i) * 1024 * 1024 )
            uploadFile( s"$originalName/$i", buf )
            buf
        }

        // A download with concatenattion should fail because due to parts having different sizes
        intercept[AssertionError] {
            download( s3Original, hdfsOriginal, concat = true ).run( )
        }

        // Check that the individual files of a non-concatenated download equal the originals
        download( s3Original, hdfsOriginal, concat = false ).run( )
        bufs.zipWithIndex.foreach { case (buf, i) =>
            assertFile( new URI( hdfsOriginal.toString + s"/$i" ), buf )
        }
        val concatName = prefix + "_uploadConcat"
        upload( hdfsOriginal, new URI( s3Prefix + concatName ), concat = true ).run( )
        assertByteArrayEquals( downloadFile( concatName ), bufs.reduce( _ ++ _ ) )

        // Check that the individual files of a non-concatenated upload equal the originals
        val uploadName = prefix + "_upload"
        upload( hdfsOriginal, new URI( s3Prefix + "" + uploadName ), concat = false ).run( )
        bufs.zipWithIndex.foreach { case (buf, i) =>
            assertByteArrayEquals( downloadFile( s"$uploadName/part-0000${i + 1}" ), buf )
        }
    }

    def uploadFile( name: String, bytes: Array[Byte] ) =
    {
        val fos = new ByteArrayInputStream( bytes )
        try {
            val metadata: ObjectMetadata = new ObjectMetadata( )
            metadata.setContentLength( bytes.length )
            s3.putObject( testDirName, name, fos, metadata )
        } finally {
            fos.close( )
        }
    }

    def downloadFile( name: String ): Array[Byte] = {
        val content = s3.getObject( testDirName, name ).getObjectContent
        try {
            IOUtils.toByteArray( content )
        } finally {
            content.close( )
        }
    }

    def randomBytes( size: Int ): Array[Byte] = {
        val bytes = new Array[Byte]( size )
        val random = new Random( )
        random.nextBytes( bytes )
        bytes
    }

    def download( src: URI,
                  dst: URI,
                  concat: Boolean,
                  s3PartSize: Int = partSize,
                  hdfsBlockSize: Int = partSize ): Download =
    {
        new Download( Config(
            s3PartSize = s3PartSize, hdfsBlockSize = hdfsBlockSize,
            src = src, dst = dst,
            concat = concat ), credentials )
    }

    def upload( src: URI, dst: URI, concat: Boolean ): Upload =
    {
        new Upload( Config( src = src, dst = dst, concat = concat ), credentials )
    }

    def assertFile( url: URI, a: Array[Byte] ): Unit = {
        val hdfs = new Path( hdfsPrefix ).getFileSystem( new Configuration( ) )
        try {
            val stream: FSDataInputStream = hdfs.open( new Path( url ) )
            try
                assertByteArrayEquals( a, IOUtils.toByteArray( stream ) )
            finally
                stream.close( )
        } finally {
            hdfs.close( )
        }
    }

    def assertByteArrayEquals( a: Array[Byte], b: Array[Byte] ): Unit = {
        assert( util.Arrays.equals( a, b ) )
    }
}

