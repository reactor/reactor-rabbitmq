# Reactor RabbitMQ

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Travis CI](https://travis-ci.org/reactor/reactor-rabbitmq.svg?branch=master)](https://travis-ci.org/reactor/reactor-rabbitmq)

You need to have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

### Building Reactor RabbitMQ jars ###
    ./gradlew jar

### Running unit tests ###
    ./gradlew test

You need a local running RabbitMQ instance. 

### Building IDE project ###
    ./gradlew eclipse
    ./gradlew idea

#### To build applications using reactor-rabbitmq API: ####

With Gradle from repo.spring.io:
```groovy
    repositories {
      maven { url 'http://repo.spring.io/snapshot' }
      maven { url 'http://repo.spring.io/milestone' }
      mavenCentral()
    }

    dependencies {
      compile "io.projectreactor.rabbitmq:reactor-rabbitmq:1.0.1.BUILD-SNAPSHOT"
    }
```

### Community / Support ###

* [GitHub Issues](https://github.com/reactor/reactor-rabbitmq/issues)

### License ###

Reactor RabbitMQ is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).

_Sponsored by [Pivotal](http://pivotal.io)_
