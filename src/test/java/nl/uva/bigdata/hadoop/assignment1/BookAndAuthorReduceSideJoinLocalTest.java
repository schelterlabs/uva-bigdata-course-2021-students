package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.exercise1.BookAndAuthorJoinLocalTest;
import org.junit.Test;

public class BookAndAuthorReduceSideJoinLocalTest extends BookAndAuthorJoinLocalTest {

    @Test
    public void reduceSideJoin() throws Exception {
        testJoin(new BookAndAuthorReduceSideJoin(), false);
    }
}
