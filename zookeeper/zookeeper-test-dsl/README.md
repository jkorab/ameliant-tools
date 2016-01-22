Utility library for embedding a Zookeeper instance in a JUnit test.

Before each JUnit ``@Test`` method the library will spin up a Zookeeper instance on a dynamically-determined port,
with storage in a dynamically created directory, located within ``System.getProperty("java.tmpdir")``. At the end of 
each test, the instance will be shut down and the directory deleted.

Sample usage:

```java

    public class MyTest {
    
        @Rule
        public EmbeddedZooKeeper zookeeper = new EmbeddedZooKeeper();
    
        @Test
        public void testPortAssignment() {
            assertThat(zookeeper.getPort(), greaterThan(0));
        }
    
    }

```