package ds.mapreduce.talkingdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TextPairWritable implements WritableComparable<TextPairWritable> {

    private Text leftElement;
    private Text rightElement;

    public TextPairWritable () {}

    public TextPairWritable (Text leftElement, Text rightElement) {

        this.leftElement = leftElement;
        this.rightElement = rightElement;

    }

    public Text getLeftElement () {

        return this.leftElement;

    }

    public Text getRightElement () {

        return this.rightElement;

    }

    public void setLeftElement (Text leftElement) {

        this.leftElement = leftElement;

    }

    public void setRightElement (Text rightElement) {

        this.rightElement = rightElement;

    }

    public void setElements (Text leftElement, Text rightElement) {

        this.leftElement = leftElement;
        this.rightElement = rightElement;

    }

    public void write (DataOutput output) throws IOException {
        
        this.leftElement.write(output);
        this.rightElement.write(output);
    }

    public void readFields (DataInput input) throws IOException {

        this.leftElement = new Text();
        this.leftElement.readFields(input);

        this.rightElement = new Text();
        this.rightElement.readFields(input);

    }

    public int compareTo (TextPairWritable textPairWritable) {

        int comparison;

        comparison = this.leftElement.compareTo(textPairWritable.leftElement);
        if (comparison == 0) {
            comparison = this.rightElement.compareTo(textPairWritable.rightElement);
        }

        return comparison;

    }

    @Override
    public int hashCode () {

        return Objects.hash(leftElement, rightElement);

    }

    public boolean equals (TextPairWritable textPairWritable) {

        return this.leftElement.equals(textPairWritable.leftElement) && this.rightElement.equals(textPairWritable.rightElement);

    }

    @Override
    public String toString () {

        return String.join(",", this.leftElement.toString(), this.rightElement.toString());

    }

}