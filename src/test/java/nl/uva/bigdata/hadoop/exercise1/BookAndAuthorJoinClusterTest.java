package nl.uva.bigdata.hadoop.exercise1;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import nl.uva.bigdata.hadoop.HadoopClusterTestCase;
import nl.uva.bigdata.hadoop.HadoopJob;
import nl.uva.bigdata.hadoop.assignment1.Book;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.regex.Pattern;


public class BookAndAuthorJoinClusterTest extends HadoopClusterTestCase {

    protected void testJoin(HadoopJob bookAndAuthorJoin, boolean mapOnly) throws Exception {
        Path authorsFile = path("authors.tsv");
        Path booksFile = path("books.tsv");
        Path outputDir = path("output");

        writeLines(authorsFile, readResource("/authors.tsv"));
        writeLines(booksFile, readResource("/books.tsv"));

        bookAndAuthorJoin.runOnCluster(createJobConf(), new String[]{"--authors", authorsFile.toString(),
                "--books", booksFile.toString(), "--output", outputDir.toString()});

        String outputFilename = mapOnly ? "part-m-00000" : "part-r-00000";

        Multimap<String, Book> booksByAuthors = readBooksByAuthors(new Path(outputDir, outputFilename));

        assertTrue(booksByAuthors.containsKey("Charles Bukowski"));
        assertTrue(booksByAuthors.get("Charles Bukowski")
                .contains(new Book("Confessions of a Man Insane Enough to Live with Beasts", 1965)));
        assertTrue(booksByAuthors.get("Charles Bukowski")
                .contains(new Book("Hot Water Music", 1983)));

        assertTrue(booksByAuthors.containsKey("Fyodor Dostoyevsky"));
        assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("Crime and Punishment", 1866)));
        assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("The Brothers Karamazov", 1880)));

    }

    Multimap<String, Book> readBooksByAuthors(Path outputFile) throws IOException {
        Multimap<String, Book> booksByAuthors = HashMultimap.create();

        Pattern separator = Pattern.compile("\t");
        for (String line : readLines(outputFile)) {
            String[] tokens = separator.split(line);
            booksByAuthors.put(tokens[0], new Book(tokens[1], Integer.parseInt(tokens[2])));
        }
        return booksByAuthors;
    }


}