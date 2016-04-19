package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Interface to local Provider.
 * Server thread uses this to talk to local provider.
 *
 * Created by kishore on 3/19/16.
 */
public class MessageStore {

    private static final String TAG = MessageStore.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final ContentResolver mContentResolver;
    private final Uri mUri;

    public MessageStore(ContentResolver cr) {
        mContentResolver = cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public void insert(String key, String val) {
        ContentValues values = new ContentValues();
        values.put(KEY_FIELD, key);
        values.put(VALUE_FIELD, val);
        mContentResolver.insert(mUri, values);
        Log.v(TAG, "insert Key: " + key + " val: " + val);
    }

    public List<Map.Entry<String, String>> query(String key) {
        List<Map.Entry<String, String>> resultList = new ArrayList<Map.Entry<String, String>>();
        Cursor resultCursor = mContentResolver.query(mUri, null, key, null, null);
        if (resultCursor == null) {
           Log.v(TAG, "query key " + key + " not found");
        } else {
            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

            // iterate through the results and add them to the text view
            while (resultCursor.moveToNext()) {
                String resultKey = resultCursor.getString(keyIndex);
                String resultVal = resultCursor.getString(valueIndex);
                String opMsg = "query key " + resultKey + " value " + resultVal;
                Log.v(TAG, opMsg);
                resultList.add(new AbstractMap.SimpleEntry<String, String>(resultKey, resultVal));
            }
            resultCursor.close();
        }
        return resultList;
    }

    /**
     *
     * @param key
     * @param nodePort
     * @param nodeId
     * @return
     */
    public int delete(String key, String nodePort, String nodeId) {
        int delCount = mContentResolver.delete(mUri, key, new String[]{nodePort, nodeId});
        Log.v(TAG, "delete count " + delCount);
        return delCount;
    }
}
