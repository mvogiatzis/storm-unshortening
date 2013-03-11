Storm-unshortening
==================


The application is built to demonstrate an example of Storm distributed
framework, currently version 0.8.2.

You can find Storm here:
http://storm-project.net

Reads from Twitter stream in real-time and unshortens the urls found in the
tweets using 3rd party calls. Then it writes the short-resolved
url pairs into a cassandra table.

This project uses Maven to build and run.

Check the [config.properties](config.properties)
and insert your own values. 
