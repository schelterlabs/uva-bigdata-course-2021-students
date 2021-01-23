package nl.uva.bigdata.hadoop.assignment1;

import com.google.common.base.Preconditions;

public class Book {

    private final String title;
    private final int year;

    public Book(String title, int year) {
        this.title = Preconditions.checkNotNull(title);
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public int getYear() {
        return year;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Book) {
            Book other = (Book) o;
            return title.equals(other.title) && year == other.year;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 31 * title.hashCode() + year;
    }

    @Override
    public String toString() {
        return "Book{" +
                "title='" + title + '\'' +
                ", year=" + year +
                '}';
    }
}
