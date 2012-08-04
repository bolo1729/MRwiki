MRwiki
======

Tools for analyzing Wikipedia content (especially links) using Python and MapReduce.

Getting started
---------------

Below is a sample session in which we download and process Wikipedia dumps
using MRwiki tools.  Run the following commands in the MRwiki directory.

    mkdir tmp
    cd tmp
    ./s1-download-dumps.sh
    for f in * ; do gzip -d f ; done
    cd ..

At this point the necessary Wikipedia dump files are downloaded and unpacked.
It is time to copy them to an HDFS filesystem.
Note: we wanted to unpack the dumps, since gzip files are not splittable.
If you *really* care about space, recompress the dump files with bzip2 (not shown here).
Warning: do not rename the dump files in any other way than by adding a suffix,
since some scripts depend on information that is encoded in the filenames.

    export WIKI=hdfs:///user/$USER/wiki
    hadoop fs -mkdir $WIKI
    hadoop fs -mkdir $WIKI/d1
    hadoop fs -put tmp/* $WIKI/d1

Great!  The SQL dumps are now on the HDFS.  Time to parse them and sort by type.
Note: we will use 100 reducers.
If you're lucky enough to have a larger cluster at your disposal,
go ahead and increase this number.
However, bear in mind that allocating more than a thousand or so reducers
will probably be a waste of resources.

    export COMMONOPTS='-r hadoop --jobconf mapred.reduce.tasks=100 --no-output'
    …
