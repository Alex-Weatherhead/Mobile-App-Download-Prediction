package ds.mapreduce.talkingdata;

import org.apache.hadoop.io.Text;

public final class Constants {

    public static final Text FIRST = new Text("a"); // Composite keys with FIRST as their right element will come first in the reducer.
    public static final Text SECOND = new Text("b"); // Composite keys with SECOND as their right element will come second in the reducer.
    public static final Text THIRD = new Text("c"); // Composite keys with THIRD as their right element will come third in the reducer.

    public static final Text EMPTY = new Text(""); 

    public static final String ATTRIBUTED = "1";

    public static final String SPACE_DELIMETER = " ";
    public static final String COMMA_DELIMETER = ",";

    private Constants () {}

}