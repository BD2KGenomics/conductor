Conductor for Apache Spark provides efficient, distributed transfers of large
files from S3 to HDFS and back.

Hadoop's distcp utility supports transfers to/from S3 but does not distribute
the download of a single large file over multiple nodes. Amazon's s3distcp is
intended to fill that gap but, to our best knowledge, hasn't not been released
as open source.

A cluster of ten r3.xlarge nodes downloaded a 288GiB file in 377 seconds to an
HDFS installation with replication factor 1, yielding an aggregate transfer
rate of 782 MiB/s. For comparison, ``distcp`` typically gives you 50-80MB/s on
that instance type. A cluster of one hundred r3.xlarge nodes downloaded that
same file in 80 seconds, yielding an aggregate transfer rate of 3.683 GiB/s.

Prerequisites
=============

Run time:

* JRE 1.7+
* Spark cluster backed by HDFS

Build time:

* JDK 1.7+
* Scala SDK 2.10
* Maven

Scala 2.11 and Java 1.8 may work, too. We simply haven't tested those, yet.

Usage
=====

Downloads::

    export AWS_ACCESS_KEY=...
    export AWS_SECRET_KEY=...
    spark-submit conductor-VERSION-distribution.jar \
                 s3://BUCKET/KEY \
                 hdfs://HOST[:PORT]/PATH \
                 [--s3-part-size <value>] \
                 [--hdfs-block-size <value>] \
                 [--concat]

Uploads::

    export AWS_ACCESS_KEY=...
    export AWS_SECRET_KEY=...
    spark-submit conductor-VERSION-distribution.jar \
                 hdfs://HOST[:PORT]/PATH \
                 s3://BUCKET/KEY \
                 [--concat]

Using the ``--concat`` flag concatenates all the parts of the files following the
upload or download. The source path can be to either a file or directory. If
the path points to a file, the parts will be created in the specified part
sizes; if it points to a directory, each part will correspond to a file in the
directory. Concatenation only works in downloader if all of the parts except
for the last one are equal-sized and multiples of the specified block size.

Tests
=====
::

    spark-submit --conf spark.driver.memory=1G \
                 --executor-memory 1G \
                 conductor-integration-tests-0.4-SNAPSHOT-distribution.jar \
                 -e -s edu.ucsc.cgl.conductor.ConductorIntegrationTests

Build
=====

::

    mvn package

Caveats
=======

* Beta-quality
* Uses Spark, not Yarn/MapReduce
* Destination must be a full ``hdfs://`` URL, the ``fs.default.name``
  property is ignored
* On failure, temporary files may be left around
* S3 credentials may be set via Java properties or environment variables as
  described in the `AWS API documentation`_ but are not read from
  ``core-site.xml``

.. _`AWS API documentation`: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html

Contributors
============

Hannes Schmidt created the first bare-bones implementation of distributed
downloads from S3 to HDFS, originally called `spark-s3-downloader`.

Clayton Sanford made the HDFS block size and S3 part size configurable, added
upload support, optional concatenation and wrote integration tests. During his
efforts the project was renamed `Conductor`.
