package nl.uva.bigdata.hadoop.assignment1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class BooksWritable  implements Writable {

    private Book[] books;

    public void setBooks(Book[] books) {
        this.books = books;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Implement me
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Implement me
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BooksWritable that = (BooksWritable) o;
        return Arrays.equals(books, that.books);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(books);
    }
}
