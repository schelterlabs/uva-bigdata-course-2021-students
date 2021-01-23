package nl.uva.bigdata.hadoop.exercise1;

import org.junit.Test;

public class BookAndAuthorMapSideJoinLocalTest extends BookAndAuthorJoinLocalTest {

    @Test
    public void mapSideJoin() throws Exception {
        testJoin(new BookAndAuthorBroadcastJoin(), true);
    }
}
