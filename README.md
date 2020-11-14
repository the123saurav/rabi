[![Build Status](https://travis-ci.com/the123saurav/rabi.svg?branch=master)](https://travis-ci.com/the123saurav/rabi)
[![Code Grade](https://www.code-inspector.com/project/16197/status/svg)](https://frontend.code-inspector.com/public/project/16197/rabi/dashboard)
[![Code Quality Score](https://www.code-inspector.com/project/16197/score/svg)](https://frontend.code-inspector.com/public/project/16197/rabi/dashboard)
[![Coverage Status](https://coveralls.io/repos/github/the123saurav/rabi/badge.svg?branch=master)](https://coveralls.io/github/the123saurav/rabi?branch=master)


# rabi
A LSM based key value database in Java.

# Project Status
Implementation is going on.
At the moment:
- PUT/DELETE is working.
- Boot is working.
- Flushing to disk is working.
- Compaction is working

# Usage
```
    final Path dataDir = Paths.get("/tmp/rabi");
    byte[] validKey = "valid".getBytes();
    byte[] value = "value".getBytes();

    final DB testDB = DBFactory.getInstance(dataDir.toString(), testLogger);
    final Config.ConfigBuilder configBuilder = new Config.ConfigBuilder();

    final CompletableFuture<Void> isOpen = testDB.open(configBuilder.build());
    isOpen.get(1, TimeUnit.SECONDS);

    testDB.put(validKey, value);

    testDB.stop()
```
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
    
