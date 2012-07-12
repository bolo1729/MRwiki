MRwiki
======

Tools for analyzing Wikipedia content (especially links) using Python and MapReduce.

Descriptions of tools
---------------------

Unless you know what you're doing, run the tools in the given order.

### Import to Hadoop

* `s0-get-list-of-wikis.sh` — downloads an up-to-date list of all Wikipedia language codes.
Does this by parsing [this page](http://meta.wikimedia.org/wiki/List_of_Wikipedias).
You can use the output to update the `LANGUAGES` variable in `s1-download-dumps.sh`.
* `s1-download-dumps.sh [<lang> [<lang> …]]` — downloads the most recent database dumps of selected language editions of Wikipedia.
By default, it downloads all the editions listed below.
You can override the selection by passing
the codes of the requested editions as command-line arguments.
Note that the script will store the dumps in the current working directory.
You should now copy all the dumps to a directory in HDFS.

### Preprocessing (Hadoop jobs)

* `s2-dumps-to-json.py` — converts all the dumps one Hadoop-friendly text file (JSON format by default).
* `s4-link-redirects.py` — …
* `s5-remove-double-redirects.py` — …
* `s6-link-langlinks.py` — …
* `s7-find-connected-components.py` — …

