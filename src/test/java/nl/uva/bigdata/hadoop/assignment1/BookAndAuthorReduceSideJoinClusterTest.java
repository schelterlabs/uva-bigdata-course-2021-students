package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.exercise1.BookAndAuthorJoinClusterTest;

public class BookAndAuthorReduceSideJoinClusterTest extends BookAndAuthorJoinClusterTest {

    public void test() throws Exception {
        testJoin(new BookAndAuthorReduceSideJoin(), false);
    }

}
