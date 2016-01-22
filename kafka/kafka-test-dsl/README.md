A fluent DSL for embedding Kafka servers in JUnit tests.

Before each JUnit ``@Test`` method the library will spin up a Kafka instance on a dynamically-determined port,
with storage in a dynamically created directory, located within ``System.getProperty("java.tmpdir")``. At the end of 
each test, the instance will be shut down and the directory deleted.

The ``EmbeddedKafkaBroker`` class provides a fluent builder API, accessible via the static ``builder()`` method. This 
provides the following properties:

* ``brokerId``
* ``hostname``
* ``port``
* ``logFlushIntervalMessages``
* ``zookeeperConnect``
* ``numPartitions`` - default partition count for all topics

Sample usage:

```java

    public class EmbeddedKafkaBrokerTest {
    
        @Rule
        public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();
    
        @Rule
        public EmbeddedKafkaBroker broker = EmbeddedKafkaBroker.builder()
                .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
                .build();
    
        @Test
        public void testLifecycle() {
            Assert.assertTrue(broker.getPort() > 0);
        }
    }

```

It is also possible to predefine topics, specifying different partition counts for them, through the ``topic()`` element:

```java

    @Rule
    public EmbeddedKafkaBroker broker = EmbeddedKafkaBroker.builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .topic("goat")
                .partitions(1)
                .replicationFactor(1)
                .property("flush.messages", "1")
            .end()
            .topic("cheese")
                .partitions(3)
            .end()
            .build();

```
