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

public class PairOfLongAndIntWritableTest {

    @Test
    public void write_readFields_FirstElementIsWrittenAndReadCorrectly () throws IOException{

        final long firstElement = 7L;
        final int secondElement = 2;
        final PairOfLongAndIntWritable pair = new PairOfLongAndIntWritable(firstElement, secondElement);

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

        final PairOfLongAndIntWritable pairA = new PairOfLongAndIntWritable(6L, 2);
        final PairOfLongAndIntWritable pairB = new PairOfLongAndIntWritable(7L, 2);
        final int expected = -1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairLessThanGivenPairInSecondElement_ReturnsNegativeOne() {

        final PairOfLongAndIntWritable pairA = new PairOfLongAndIntWritable(7L, 2);
        final PairOfLongAndIntWritable pairB = new PairOfLongAndIntWritable(7L, 3);
        final int expected = -1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairLessThanGivenPairInFirstAndSecondElement_ReturnsNegativeOne () {

        final PairOfLongAndIntWritable pairA = new PairOfLongAndIntWritable(6L, 3);
        final PairOfLongAndIntWritable pairB = new PairOfLongAndIntWritable(7L, 4);
        final int expected = -1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairEqualToGivenPair_ReturnsZero () {

        final PairOfLongAndIntWritable pairA = new PairOfLongAndIntWritable(8L, 2);
        final PairOfLongAndIntWritable pairB = new PairOfLongAndIntWritable(8L, 2);
        final int expected = 0;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairGreaterThanGivenPairInFirstElement_ReturnsPositive1 () {

        final PairOfLongAndIntWritable pairA = new PairOfLongAndIntWritable(11L, 1);
        final PairOfLongAndIntWritable pairB = new PairOfLongAndIntWritable(10L, 1);
        final int expected = +1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairGreaterThanGivenPairInSecondElement_ReturnsPositve1 () {

        final PairOfLongAndIntWritable pairA = new PairOfLongAndIntWritable(10L, 8);
        final PairOfLongAndIntWritable pairB = new PairOfLongAndIntWritable(10L, 7);
        final int expected = +1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }

    @Test
    public void compareTo_PairGreaterThanGivenPairInFirstAndSecondElement_ReturnsPositive1 () {

        final PairOfLongAndIntWritable pairA = new PairOfLongAndIntWritable(13L, 5);
        final PairOfLongAndIntWritable pairB = new PairOfLongAndIntWritable(12L, 4);
        final int expected = +1;

        int actual = pairA.compareTo(pairB);

        assertEquals(actual, expected);

    }


}