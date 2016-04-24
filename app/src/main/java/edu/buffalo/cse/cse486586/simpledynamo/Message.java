package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by kishore on 3/18/16.
 */
public class Message {
    private static final String TAG = Message.class.getSimpleName();
    private static final String DELIM = "|";

    public enum Type {READ, WRITE, REPL_READ, REPL_WRITE, DEL, READ_ALL, SUB_WRITE};

    private Type type;
    private String key; // query key
    private String value;
    private int version; //value version

    private int mPort;

    private List<Map.Entry<String, String>> result = new ArrayList<Map.Entry<String, String>>(); // query result

    public Message(Type type, String key, String val, int ver, int port) {
        this.type = type;
        this.key = key;
        this.value = val;
        this.version = ver;
        this.mPort = port;
    }

    public Message(String msg) {
        parseMessage(msg);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public List<Map.Entry<String, String>> getResult() {
        return result;
    }

    public void setResult(List<Map.Entry<String, String>> result) {
        this.result = result;
    }

    public int getmPort() {
        return mPort;
    }

    public void setmPort(int mPort) {
        this.mPort = mPort;
    }

    /**
     * Method to serialize message into string
     *
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.type.toString() + DELIM);
        sb.append(this.key + DELIM);
        sb.append(this.value + DELIM);
        sb.append(this.version + DELIM);
        sb.append(this.result.size() + DELIM);
        Iterator<Map.Entry<String, String>> iter = result.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            sb.append(entry.getKey() + DELIM);
            sb.append(entry.getValue() + DELIM);
        }
        return sb.toString();
    }

    /**
     * Method to parse the string into message object
     *
     * @param rcvMessage
     */
    private void parseMessage(String rcvMessage) {
        String[] vals = rcvMessage.split("\\"+DELIM);
        this.type = Type.valueOf(vals[0]);
        this.key = vals[1];
        this.value = vals[2];
        this.version = Integer.parseInt(vals[3]);
        int resultSize = Integer.parseInt(vals[4]) * 2;
        for (int i = 0; i < resultSize; i+=2) {
            Map.Entry<String, String> entry = new AbstractMap.SimpleEntry<String, String>(vals[i + 5], vals[i + 6]);
            result.add(entry);
        }
    }
}
