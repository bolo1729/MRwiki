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
* `s2-dumps-to-hadoop-input.py <dumps-dir>` — converts dumps in the directory specified by the first argument to a Hadoop-friendly text file.
Logs go to standard error (as defined in `logging.conf`) while the (large) text file is printed on the standard output.
You can redirect the standard output to a file, and copy that file to HDFS.

### Preprocessing

* `s4-link-redirects.py` — …
* `s5-remove-double-redirects.py` — …
* `s6-link-langlinks.py` — …
* `s7-find-connected-components.py` — …

