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
Note: we will use 90 reducers.
If you're lucky enough to have a larger cluster at your disposal,
go ahead and increase this number.
However, bear in mind that allocating more than a thousand or so reducers
will probably be a waste of resources.

    export COMMONOPTS=-r hadoop --jobconf mapred.reduce.tasks=100
    python s2-dumps-to-json.py $WIKI/d1 --output-dir $WIKI/d2 $COMMONOPTS
    python s3-select-relations.py $WIKI/d2 --output-dir $WIKI/d3p --rel-type p $COMMONOPTS
    python s3-select-relations.py $WIKI/d2 --output-dir $WIKI/d3r --rel-type r $COMMONOPTS
    python s3-select-relations.py $WIKI/d2 --output-dir $WIKI/d3ll --rel-type ll $COMMONOPTS
    python s3-select-relations.py $WIKI/d2 --output-dir $WIKI/d3pl --rel-type pl $COMMONOPTS

Now we have four directories: `d3p` with page titles, `d3r` with raw redirects, `d3ll` with raw langlinks, and `d3pl` with raw pagelinks.
By "raw" we mean that the links are from pages referenced by _identifiers_ to pages referenced by _titles_.
We don't like it, it's better to have links with identifiers on both sides.
Also, we are interested only in pages from the 0th namespace, i.e., in articles.

    python s4-match-links.py $WIKI/d3p $WIKI/d3r --output-dir $WIKI/d4r --rel-type r $COMMONOPTS
    python s4-match-links.py $WIKI/d3p $WIKI/d3ll --output-dir $WIKI/d4ll --rel-type ll $COMMONOPTS
    python s4-match-links.py $WIKI/d3p $WIKI/d3pl --output-dir $WIKI/d4pl --rel-type pl $COMMONOPTS

Excellent!  Now all our links are represented in the form: `fromId relType toId`.
Let's focus on redirects for a moment.  At this point we most probably have double redirects,
triple redirects, even circular ones!  Let's clean that up: relink all redirects to the final pages
and remove all circular redirects.

    python s5-manage-dbl-redirects.py $WIKI/d4r --output-dir $WIKI/d5t1 --shorten 1 $COMMONOPTS
    python s5-manage-dbl-redirects.py $WIKI/d5t1 --output-dir $WIKI/d5t2 --shorten 1 $COMMONOPTS
    python s5-manage-dbl-redirects.py $WIKI/d5t2 --output-dir $WIKI/d5t3 --shorten 1 $COMMONOPTS
    python s5-manage-dbl-redirects.py $WIKI/d5t3 --output-dir $WIKI/d5t4 --shorten 1 $COMMONOPTS
    python s5-manage-dbl-redirects.py $WIKI/d5t4 --output-dir $WIKI/d5r $COMMONOPTS

Redirects are now reliable.  Let's use them to perform other sanity checks.
For example, if a redirect and a different link (lagnlink, pagelink)
originates from a page, we remove all the links but the redirect.
If a langlink connects two pages from the same langage edition, we remove it.

    python s6-check-links.py $WIKI/d5r $WIKI/d4ll --output-dir $WIKI/d6ll $COMMONOPTS
    python s6-check-links.py $WIKI/d5r $WIKI/d4pl --output-dir $WIKI/d6pl $COMMONOPTS

Occasionally we will prefer to know where a given link effectively leads to.
E.g. instead of a langlink to a redirect to a page, we will prefer a langlink to the final page.
Let's calculate such effective links.

    python s7-effective-links.py $WIKI/d5r $WIKI/d6ll --output-dir $WIKI/d7ll $COMMONOPTS
    python s7-effective-links.py $WIKI/d5r $WIKI/d6pl --output-dir $WIKI/d7pl $COMMONOPTS

Technically, langlinks are directed, but intended to be symmetric.
Let us perform symmetric closure on langlinks.

    python s8-symmetric-links.py $WIKI/d7ll --output-dir $WIKI/d8ll $COMMONOPTS

Let us now try to estimate the credibility of individal langlinks.
Here's the idea: we're not sure whether the langlink en:France -- fr:France is correct or not,
so we look at their surroundings: pagelinks originating from en:France and from fr:France
(e.g. en:Paris, en:English channel, fr:Paris, fr:Manche (mer)),
and langlinks between targets of these pagelinks.
We will say that langlinks like en:Paris -- fr:Paris and en:English channel -- fr:Manche (mer)
are *supporting* en:France -- fr:France.
Here is how we calculate the number of supporting langlinks for each langlink.

    python s9-find-support.py $WIKI/d6pl $WIKI/d8ll --output-dir $WIKI/d9llS $COMMONOPTS

