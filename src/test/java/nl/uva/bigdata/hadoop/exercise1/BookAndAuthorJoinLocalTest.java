package nl.uva.bigdata.hadoop.exercise1;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import nl.uva.bigdata.hadoop.HadoopLocalTestCase;
import nl.uva.bigdata.hadoop.HadoopJob;
import nl.uva.bigdata.hadoop.assignment1.Book;


import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class BookAndAuthorJoinLocalTest extends HadoopLocalTestCase {

    protected void testJoin(HadoopJob bookAndAuthorJoin, boolean mapOnly) throws Exception {
        File authorsFile = getTestTempFile("authors.tsv");
        File booksFile = getTestTempFile("books.tsv");
        File outputDir = getTestTempDir("output");
        outputDir.delete();

        writeLines(authorsFile, readLines("/authors.tsv"));
        writeLines(booksFile, readLines("/books.tsv"));

        bookAndAuthorJoin.runLocal(new String[]{"--authors", authorsFile.getAbsolutePath(),
                "--books", booksFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath()});

        String outputFilename = mapOnly ? "part-m-00000" : "part-r-00000";

        Multimap<String, Book> booksByAuthors = readBooksByAuthors(new File(outputDir, outputFilename));

        assertTrue(booksByAuthors.containsKey("Charles Bukowski"));
        assertTrue(booksByAuthors.get("Charles Bukowski")
                .contains(new Book("Confessions of a Man Insane Enough to Live with Beasts", 1965)));
        assertTrue(booksByAuthors.get("Charles Bukowski")
                .contains(new Book("Hot Water Music", 1983)));

        assertTrue(booksByAuthors.containsKey("Fyodor Dostoyevsky"));
        assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("Crime and Punishment", 1866)));
        assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("The Brothers Karamazov", 1880)));

    }

    Multimap<String, Book> readBooksByAuthors(File outputFile) throws IOException {
        Multimap<String, Book> booksByAuthors = HashMultimap.create();

        Pattern separator = Pattern.compile("\t");
        for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
            String[] tokens = separator.split(line);
            booksByAuthors.put(tokens[0], new Book(tokens[1], Integer.parseInt(tokens[2])));
        }
        return booksByAuthors;
    }


}