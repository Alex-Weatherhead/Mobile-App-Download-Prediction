package ds.mapreduce.talkingdata;

import org.kohsuke.args4j.spi.DelimitedOptionHandler;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.Setter;
import org.kohsuke.args4j.spi.IntOptionHandler;
    
/**
 * Courtesy of Marcin Gryszko at https://grysz.com/2016/03/03/multi-value-option-handler-in-args4j/
 */
public class MultiIntOptionHandler extends DelimitedOptionHandler<Integer> {
        
    public MultiIntOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Integer> setter) {
        super(parser, option, setter, ",", new IntOptionHandler(parser, option, setter));
    }

}