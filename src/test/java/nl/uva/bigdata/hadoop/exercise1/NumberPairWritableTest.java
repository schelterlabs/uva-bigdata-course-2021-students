package nl.uva.bigdata.hadoop.exercise1;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class NumberPairWritableTest {

    @Test
    public void writeAndRead() throws IOException {

        NumberPairWritable original = new NumberPairWritable(13, 27);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(buffer);
        original.write(out);

        DataInput in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));

        NumberPairWritable clone = new NumberPairWritable();
        clone.readFields(in);

        assertEquals(original, clone);
    }

}