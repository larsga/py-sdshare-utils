sdshare-server
==============

A simple [SDShare](http://www.sdshare.org) server written in Python.
It implements the 2012-07-10 draft, and has support for plugging in
different types of backends, but at the moment the only supported
backend is one for reading CSV files.

Prerequisites: [web.py](http://webpy.org)

To try it out, run the following:

    python sdshare-server.py 7000

Then navigate to http://localhost:7000 and browse the Atom feeds
produced by the server.