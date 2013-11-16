Storm-unshortening
==================


The application is built to demonstrate an example of Storm distributed
framework, currently of stable version 0.8.2.

You can find Storm at the following url along with installation instructions:
http://storm-project.net

The application reads the public Twitter stream (~1%) in real-time and unshortens the urls found in the
tweets using 3rd party calls. Then it writes the short-expanded url pairs into a cassandra table.

This project uses Maven to build and run. You can also run it in Eclipse if you import it.

Check the [config.properties](config.properties) file. You will need to create a simple Twitter app [here](https://dev.twitter.com/) and insert your own auth values.
This is necessary to consume the public stream or perform any other twitter-specific operations.

License
-------

Feel free to copy, alter or distribute this project.
See (LICENSE.md)[LICENSE.md] for licensing information.
