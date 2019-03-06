package ds.mapreduce.talkingdata.exceptions;

/**
 * 
 */
public class MissingConfigurationPropertyException extends RuntimeException {

    public MissingConfigurationPropertyException () {

        super();

    }

    public MissingConfigurationPropertyException (String message) {

        super(message);

    }

    public MissingConfigurationPropertyException (String message, Throwable cause) {

        super(message, cause);

    }

}