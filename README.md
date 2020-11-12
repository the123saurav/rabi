[![Build Status](https://travis-ci.com/the123saurav/rabi.svg?branch=master)](https://travis-ci.com/the123saurav/rabi)
# rabi
A LSM based key value database in Java.

# Project Status
Implementation is going on.
At the moment:
- PUT/DELETE is working.
- Boot is working.
- Flushing to disk is working.
- Compaction is working

# Building project
This project uses maven for builds. 
Requirements:
- maven CLI(tested with 3.6.2)
- Java 8

# Code Coverage
Run mvn test
Then go to target/site/jacoco/index.html

# Style Convention
We follow [google style guide](https://google.github.io/styleguide/javaguide.html). The corresponding checksyles config
can be found in config dir. You can use the file with your preferred ide e.g this can be imported in "code styles: java"
config in intellij.
    
