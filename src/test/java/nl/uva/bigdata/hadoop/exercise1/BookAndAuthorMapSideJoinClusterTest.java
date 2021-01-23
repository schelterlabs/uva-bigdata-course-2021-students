package nl.uva.bigdata.hadoop.exercise1;

public class BookAndAuthorMapSideJoinClusterTest extends BookAndAuthorJoinClusterTest {



    public void test() throws Exception {
        testJoin(new BookAndAuthorBroadcastJoin(), true);
    }
}
