# Reactor RabbitMQ

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Travis CI](https://travis-ci.org/reactor/reactor-rabbitmq.svg?branch=master)](https://travis-ci.org/reactor/reactor-rabbitmq)

Reactor RabbitMQ is a reactive API for [RabbitMQ](http://www.rabbitmq.com/) based on
[Reactor](http://projectreactor.io/)
and [RabbitMQ Java Client](http://www.rabbitmq.com/api-guide.html). Reactor RabbitMQ API enables messages to be
published to RabbitMQ and consumed from RabbitMQ using functional APIs with
non-blocking back-pressure and very low overheads. This enables applications
using Reactor to use RabbitMQ as a message bus or streaming platform and integrate
with other systems to provide an end-to-end reactive pipeline.

## Getting started

For the latest stable release, please see the [getting started](http://projectreactor.io/docs/rabbitmq/milestone/stable/#_getting_started)
section in the [reference documentation](http://projectreactor.io/docs/rabbitmq/stable/reference/).

For latest milestone:
[Getting Started](http://projectreactor.io/docs/rabbitmq/milestone/reference/#_getting_started)
[Reference Documentation](http://projectreactor.io/docs/rabbitmq/milestone/reference/)

For snapshots:
[Getting Started](http://projectreactor.io/docs/rabbitmq/snapshot/reference/#_getting_started) -
[Reference Documentation](http://projectreactor.io/docs/rabbitmq/snapshot/reference/)

## Documentation

[Reference documentation](http://projectreactor.io/docs/rabbitmq/milestone/reference/)

[Javadoc](http://projectreactor.io/docs/rabbitmq/milestone/api/index.html)

## Building applications using Reactor RabbitMQ API

You need to have [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

With Maven:
```xml
<dependency>
    <groupId>io.projectreactor.rabbitmq</groupId>
    <artifactId>reactor-rabbitmq</artifactId>
    <version>1.0.0.RELEASE</version>
</dependency>
```


With Gradle:
```groovy
dependencies {
  compile "io.projectreactor.rabbitmq:reactor-rabbitmq:1.0.0.RELEASE"
}
```

## Milestones and release candidates

With Maven:
```xml
<dependency>
    <groupId>io.projectreactor.rabbitmq</groupId>
    <artifactId>reactor-rabbitmq</artifactId>
    <version>1.1.0.RC2</version>
</dependency>

<repositories>
    <repository>
        <id>spring-milestones</id>
        <name>Spring Milestones</name>
        <url>https://repo.spring.io/milestone</url>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
</repositories>
```


With Gradle:
```groovy
repositories {
  maven { url 'https://repo.spring.io/milestone' }
  mavenCentral()
}

dependencies {
  compile "io.projectreactor.rabbitmq:reactor-rabbitmq:1.1.0.RC2"
}
```

## Snapshots

With Maven:
```xml
<dependency>
    <groupId>io.projectreactor.rabbitmq</groupId>
    <artifactId>reactor-rabbitmq</artifactId>
    <version>1.1.0.BUILD-SNAPSHOT</version>
</dependency>

<repositories>
    <repository>
        <id>spring-snapshots</id>
        <name>Spring Snapshots</name>
        <url>https://repo.spring.io/libs-snapshot</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

With Gradle:
```groovy
repositories {
  maven { url 'https://repo.spring.io/libs-snapshot' }
  mavenCentral()
}

dependencies {
  compile "io.projectreactor.rabbitmq:reactor-rabbitmq:1.1.0.BUILD-SNAPSHOT"
}
```

## Build intructions

### Building Reactor RabbitMQ jars
    ./gradlew jar

### Running unit tests
    ./gradlew test

You need a local running RabbitMQ instance. 

### Building IDE project
    ./gradlew eclipse
    ./gradlew idea

## Community / Support

* [GitHub Issues](https://github.com/reactor/reactor-rabbitmq/issues)

## License ##

Reactor RabbitMQ is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).

_Sponsored by [Pivotal](http://pivotal.io)_
