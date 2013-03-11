Storm-unshortening
==================

Reads from Twitter stream in real-time and unshortens the urls found in the
tweets using 3rd party calls. Then the next step is to write the short-resolved
url pairs into a cassandra table.

The application is built to demonstrate an example of Storm distributed 
framework, currently version 0.8.2.

http://storm-project.net

This project uses Maven to build and run.

Check the [config.properties](https://github.com/mvogiatzis/storm-unshortening/config.properties)
and insert your own values. 
