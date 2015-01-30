package nl.minvenj.nfi.storm.kafka.util;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Describe...
 *
 * @author <a href="mailto:alan.ma@disney.com">Alan Ma</a>
 * @version 1.0
 */
public class StringScheme implements Scheme {
    public static final String STRING_SCHEME_KEY = "str";

    public StringScheme() {
    }

    public List<Object> deserialize(byte[] bytes) {
        return new Values(new Object[]{deserializeString(bytes)});
    }

    public static String deserializeString(byte[] string) {
        try {
            return new String(string, "UTF-8");
        } catch (UnsupportedEncodingException var2) {
            throw new RuntimeException(var2);
        }
    }

    public Fields getOutputFields() {
        return new Fields(new String[]{"str"});
    }
}