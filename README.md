# Reactor RabbitMQ

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Reactor RabbitMQ](https://maven-badges.herokuapp.com/maven-central/io.projectreactor.rabbitmq/reactor-rabbitmq/badge.svg?style=plastic)](https://mvnrepository.com/artifact/io.projectreactor.rabbitmq/reactor-rabbitmq)

[![Build Status](https://github.com/reactor/reactor-rabbitmq/workflows/Build%20(Linux)/badge.svg?branch=master)](https://github.com/reactor/reactor-rabbitmq/actions?query=workflow%3A%22Build+%28Linux%29%22+branch%3Amaster) (master)
[![Build Status](https://github.com/reactor/reactor-rabbitmq/workflows/Build%20(Linux)/badge.svg?branch=1.4.x)](https://github.com/reactor/reactor-rabbitmq/actions?query=workflow%3A%22Build+%28Linux%29%22+branch%3A1.4.x) (1.4.x)

Reactor RabbitMQ is a reactive API for [RabbitMQ](https://www.rabbitmq.com/) based on
[Reactor](https://projectreactor.io/)
and [RabbitMQ Java Client](https://www.rabbitmq.com/api-guide.html). Reactor RabbitMQ API enables messages to be
published to RabbitMQ and consumed from RabbitMQ using functional APIs with
non-blocking back-pressure and very low overheads. This enables applications
using Reactor to use RabbitMQ as a message bus or streaming platform and integrate
with other systems to provide an end-to-end reactive pipeline.

## Getting started

For the latest stable release, please see the [getting started](https://projectreactor.io/docs/rabbitmq/release/reference/#_getting_started)
section in the [reference documentation](https://projectreactor.io/docs/rabbitmq/release/reference/).
You can view the [Javadoc](https://projectreactor.io/docs/rabbitmq/release/api/index.html) as well.

For latest milestone:
[Getting Started](https://projectreactor.io/docs/rabbitmq/milestone/reference/#_getting_started) -
[Reference Documentation](https://projectreactor.io/docs/rabbitmq/milestone/reference/) -
[Javadoc](https://projectreactor.io/docs/rabbitmq/milestone/api/index.html)

For snapshots:
[Getting Started](https://projectreactor.io/docs/rabbitmq/snapshot/reference/#_getting_started) -
[Reference Documentation](https://projectreactor.io/docs/rabbitmq/snapshot/reference/) -
[Javadoc](https://projectreactor.io/docs/rabbitmq/snapshot/api/index.html)

## Building applications using Reactor RabbitMQ API

You need to have [Java 8](https://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

With Maven:
```xml
<dependency>
    <groupId>io.projectreactor.rabbitmq</groupId>
    <artifactId>reactor-rabbitmq</artifactId>
    <version>1.5.5</version>
</dependency>
```


With Gradle:
```groovy
dependencies {
  compile "io.projectreactor.rabbitmq:reactor-rabbitmq:1.5.5"
}
```

## Milestones and release candidates

With Maven:
```xml
<dependency>
    <groupId>io.projectreactor.rabbitmq</groupId>
    <artifactId>reactor-rabbitmq</artifactId>
    <version>1.5.5</version>
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
  compile "io.projectreactor.rabbitmq:reactor-rabbitmq:1.5.5"
}
```

## Snapshots

With Maven:
```xml
<dependency>
    <groupId>io.projectreactor.rabbitmq</groupId>
    <artifactId>reactor-rabbitmq</artifactId>
    <version>1.5.6-SNAPSHOT</version>
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
  compile "io.projectreactor.rabbitmq:reactor-rabbitmq:1.5.6-SNAPSHOT"
}
```

## Build instructions

### Building Reactor RabbitMQ jars
    ./gradlew jar

### Running tests

The test suite needs to execute `rabbitmqctl` to test connection recovery. You
can specify the path to `rabbitmqctl` like the following:

    ./gradlew check -Drabbitmqctl.bin=/path/to/rabbitmqctl

You need a local running RabbitMQ instance.

### Running tests with Docker

Start a RabbitMQ container:

    docker run -it --rm --name rabbitmq -p 5672:5672 rabbitmq:3.9

Run the test suite:

    ./gradlew check -i -s

### Building IDE project
    ./gradlew eclipse
    ./gradlew idea

## Versioning

Reactor RabbitMQ used [semantic versioning](https://semver.org/) from version 1.0 to version 1.4, but switched to
another scheme for consistency with [Reactor Core](https://github.com/reactor/reactor-core/)
and the other Reactor libraries.

Starting from 1.4, Reactor RabbitMQ uses a `GENERATION.MAJOR.MINOR` scheme, whereby an increment in:

 * `GENERATION` marks a change of library generation. Expect improvements, new features, bug fixes, and
 incompatible API changes.
 * `MAJOR` marks a significant release. Expect new features, bug fixes, and small incompatible API changes.
 * `MINOR` marks a maintenance release. Expect new features and bug fixes, but *no* incompatible API changes.

## Community / Support

* For Reactor and Reactor RabbitMQ questions: [![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* For RabbitMQ questions: [RabbitMQ Users](https://groups.google.com/forum/#!forum/rabbitmq-users)
* For reporting bugs and feature requests: [GitHub Issues](https://github.com/reactor/reactor-rabbitmq/issues)

See the [RabbitMQ Java libraries support page](https://www.rabbitmq.com/java-versions.html)
for the support timeline of this library.

## License ##

Reactor RabbitMQ is [Apache 2.0 licensed](https://www.apache.org/licenses/LICENSE-2.0.html).

_Sponsored by [VMware](https://tanzu.vmware.com/)_
