package nl.uva.bigdata.hadoop;

import com.google.common.io.Closeables;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HadoopClusterTestCase extends ClusterMapReduceTestCase  {

    public void doSetup() throws Exception {
        super.setUp();
    }

    public void doTearDown() throws Exception {
        super.tearDown();
    }

    protected Path path(String path) {
        return new Path(createJobConf().get("fs.default.name") + "/" + path);
    }

    protected List<String> readLines(Path file) throws IOException {

        List<String> lines = new ArrayList<>();

        FSDataInputStream in = getFileSystem().open(file);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            while(reader.ready()) {
                lines.add(reader.readLine());
            }
        }

        return lines;
    }

    protected void writeLines(Path file, String... lines) throws IOException {
        this.writeLines(file, Arrays.asList(lines));
    }

    protected void writeLines(Path file, Iterable<String> lines) throws IOException {

        FSDataOutputStream out = getFileSystem().create(file, true);

        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            for (String line : lines) {
                writer.println(line);
            }
        }
    }

    public List<String> readResource(String path) throws IOException {
        List<String> lines = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(path)));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } finally {
            Closeables.closeQuietly(reader);
        }
        return lines;
    }
}
