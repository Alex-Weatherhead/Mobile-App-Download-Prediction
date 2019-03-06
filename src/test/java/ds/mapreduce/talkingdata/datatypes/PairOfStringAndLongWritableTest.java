package ds.mapreduce.talkingdata.datatypes;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

public class PairOfStringAndLongWritableTest {

    @Test
    public void write_readFields_FirstElementIsWrittenAndReadCorrectly () throws IOException {

        final String firstElement = "mango";
        final long secondElement = 11L;
        final PairOfStringAndLongWritable pair = new PairOfStringAndLongWritable(firstElement, secondElement);

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final DataOutput dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        
        pair.write(dataOutputStream);

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final DataInput dataInputStream = new DataInputStream(byteArrayInputStream);

        pair.readFields(dataInputStream);

        assertEquals(firstElement, pair.getFirstElement());
        assertEquals(secondElement, pair.getSecondElement());

    }

    @Test
    public void compareTo_PairLessThanGivenPairInFirstElement_ReturnsNegativeOne () {

        final PairOfStringAndLongWritable pairA = new PairOfStringAndLongWritable("apple", 8L);
        final PairOfStringAndLongWritable pairB = new PairOfStringAndLongWritable("banana", 8L);
        final int expected = -1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairLessThanGivenPairInSecondElement_ReturnsNegativeOne() {

        final PairOfStringAndLongWritable pairA = new PairOfStringAndLongWritable("banana", 3L);
        final PairOfStringAndLongWritable pairB = new PairOfStringAndLongWritable("banana", 4L);
        final int expected = -1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairLessThanGivenPairInFirstAndSecondElement_ReturnsNegativeOne () {

        final PairOfStringAndLongWritable pairA = new PairOfStringAndLongWritable("apple", 1L);
        final PairOfStringAndLongWritable pairB = new PairOfStringAndLongWritable("banana", 2L);
        final int expected = -1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairEqualToGivenPair_ReturnsZero () {

        final PairOfStringAndLongWritable pairA = new PairOfStringAndLongWritable("orange", 9L);
        final PairOfStringAndLongWritable pairB = new PairOfStringAndLongWritable("orange", 9L);
        final int expected = 0;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairGreaterThanGivenPairInFirstElement_ReturnsPositive1 () {

        final PairOfStringAndLongWritable pairA = new PairOfStringAndLongWritable("banana", 2L);
        final PairOfStringAndLongWritable pairB = new PairOfStringAndLongWritable("apple", 2L);
        final int expected = +1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairGreaterThanGivenPairInSecondElement_ReturnsPositve1 () {

        final PairOfStringAndLongWritable pairA = new PairOfStringAndLongWritable("banana", 6L);
        final PairOfStringAndLongWritable pairB = new PairOfStringAndLongWritable("banana", 5L);
        final int expected = +1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairGreaterThanGivenPairInFirstAndSecondElement_ReturnsPositive1 () {

        final PairOfStringAndLongWritable pairA = new PairOfStringAndLongWritable("banana", 3L);
        final PairOfStringAndLongWritable pairB = new PairOfStringAndLongWritable("apple", 2L);
        final int expected = +1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

}