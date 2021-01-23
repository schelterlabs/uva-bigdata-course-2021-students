package nl.uva.bigdata.hadoop.assignment1;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BooksWritableTest {

    @Test
    public void writeAndRead() throws IOException {

        Book[] books = { new Book("Homage to Catalonia", 1938),
            new Book("Crime and Punishment", 1866), new Book("De Bello Gallico", -58)
        };

        Book[] moreBooks = { new Book("The Gambler", 1867), new Book("Nineteen Eighty-Four", 1949) };

        BooksWritable original = new BooksWritable();
        original.setBooks(books);

        BooksWritable noBooks = new BooksWritable();
        noBooks.setBooks(new Book[] {});

        BooksWritable originalMore = new BooksWritable();
        originalMore.setBooks(moreBooks);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(buffer);

        original.write(out);
        noBooks.write(out);
        originalMore.write(out);

        DataInput in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));

        BooksWritable clone = new BooksWritable();
        BooksWritable noBooksClone = new BooksWritable();
        BooksWritable cloneMore = new BooksWritable();

        clone.readFields(in);
        noBooksClone.readFields(in);
        cloneMore.readFields(in);

        assertEquals(original, clone);
        assertEquals(noBooks, noBooksClone);
        assertEquals(originalMore, cloneMore);
    }

}
