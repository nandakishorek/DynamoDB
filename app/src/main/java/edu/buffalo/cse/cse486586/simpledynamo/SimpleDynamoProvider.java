package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.ConditionVariable;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getName();

	// column names
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final String[] COLUMN_NAMES = {KEY_FIELD, VALUE_FIELD};
    private static final int[] REMOTE_PORTS = {11108, 11112, 11116, 11120, 11124};

    private TreeMap<String, Integer> mNodeMap;

	static final int SERVER_PORT = 10000;

	private int mPort; // the port of this node
	private String mNodeId; // hash of the emulator port, ex. hash("5554");
	private ServerTask mServerTask;
    private List<Integer> preflist; // preference list for replication
    private List<Integer> prevNodes;

    Executor mExecutor = Executors.newFixedThreadPool(20);

    @Override
    public boolean onCreate() {
        Log.v(TAG, "onCreate");

        // determine the port of this node
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        this.mPort = Integer.parseInt(portStr) * 2;
        Log.v(TAG,"SimpleDynamoProvider port " + mPort);

        // gen node id
        try {
            mNodeId = HashUtility.genHash(Integer.toString(mPort / 2));
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "onCreate: SHA-1 not supported");
        }

        // start the server thread
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 10);
            mServerTask = new ServerTask(mPort, mNodeId, this, getContext().getContentResolver());
            mServerTask.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket - " + e.getMessage());
        }

        // generate the node hashes
        mNodeMap = new TreeMap<>();
        for (int port: REMOTE_PORTS) {
            try {
                String hash = HashUtility.genHash(Integer.toString(port / 2));
                mNodeMap.put(hash, port);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "onCreate: SHA-1 not supported");
            }
        }

        // generate the preference list
        preflist = new ArrayList<>();
        String firstSuccessor = mNodeMap.higherKey(mNodeId);
        if (firstSuccessor == null) {
            firstSuccessor = mNodeMap.firstKey();
        }
        preflist.add(mNodeMap.get(firstSuccessor));


        String second = mNodeMap.higherKey(firstSuccessor);
        if (second == null) {
            second = mNodeMap.firstKey();
        }
        preflist.add(mNodeMap.get(second));

        // sync from successor and predecessor
        String predecessor = mNodeMap.lowerKey(mNodeId);
        if (predecessor == null) {
            predecessor = mNodeMap.lastKey();
        }

        // get the previous two nodes
        prevNodes = new ArrayList<>();
        prevNodes.add(mNodeMap.get(predecessor));
        String predToPred = mNodeMap.lowerKey(predecessor);
        if (predToPred == null) {
            predToPred = mNodeMap.lastKey();
        }
        prevNodes.add(mNodeMap.get(predToPred));

        Log.v(TAG, "Previous nodes " + prevNodes.toString());
        sync(mNodeMap.get(firstSuccessor), mNodeMap.get(predecessor));

        return true;
    }

    /**
     * Returns the port of the coordinator node responsible for this key
     *
     * @param key the key
     * @return the port of the co-ordinator
     */
    private int getCoOrdinator(String key) {
        try {
            String keyHash = HashUtility.genHash(key);
            for (String hash : mNodeMap.keySet()) {
                if (keyHash.compareTo(hash) <= 0) {
                    return mNodeMap.get(hash);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "SHA-1 not supported");
        }
        return mNodeMap.get(mNodeMap.firstKey());
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        int coOrdPort = getCoOrdinator(selection);

        if("@".equals(selection)) {
            deleteLocal(selection);
        } else if ("*".equals(selection)) {
            deleteLocal(selection);
            Message message = new Message(Message.Type.DEL, selection, null, 0, coOrdPort);
            new DeleteAllTask().executeOnExecutor(mExecutor, message);
        } else if (coOrdPort == mPort){
            deleteLocal(selection);
            Message message = new Message(Message.Type.DEL, selection, null, 0, coOrdPort);
            new DeleteTask().executeOnExecutor(mExecutor, message);
        } else {
            Message message = new Message(Message.Type.DEL, selection, null, 0, coOrdPort);
            try {
                new ForwardTask().executeOnExecutor(mExecutor, message).get();

                if (message.getKey() == null) {
                    // coordinator did not ACK, then send it to the node after that
                    try {
                        Map.Entry<String, Integer> firstSuccessor = mNodeMap.higherEntry(HashUtility.genHash(Integer.toString(coOrdPort/2)));
                        if (firstSuccessor == null) {
                            firstSuccessor = mNodeMap.firstEntry();
                        }

                        message.setType(Message.Type.SUB_DEL);
                        message.setKey(selection);
                        message.setmPort(firstSuccessor.getValue());

                        Log.v(TAG, "Forwarding to first successor of coordinator " + firstSuccessor.getValue());
                        new ForwardTask().executeOnExecutor(mExecutor, message).get();
                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "onCreate: SHA-1 not supported");
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

		return 0;
	}

    public int deleteLocal(String key) {
        if (key.equals("*") || key.equals("@")) {
            // delete everything
            String[] allKeys = getContext().fileList();
            for (String k : allKeys) {
                getContext().deleteFile(k);
                Log.v(TAG, "delete local key " + k);
            }
            return allKeys.length;
        } else if (getContext().deleteFile(key)) {
            Log.v(TAG, "delete local key " + key);
            return 1;
        }
        Log.v(TAG, "delete local key " + key + " not found");
        return 0;
    }

    public void deleteFromSuccessor(String key) {
        Log.v(TAG, "deleteFromSuccessor " + key);
        Message message = new Message(Message.Type.DEL, key, null, 0, preflist.get(0));
        try {
            new ForwardTask().executeOnExecutor(mExecutor, message).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String key = (String) values.get(KEY_FIELD);
        String val = (String) values.get(VALUE_FIELD);

        int coOrdPort = getCoOrdinator(key);
        if (coOrdPort != mPort) {
            // send it to the right node
            Message message = new Message(Message.Type.WRITE, key, val, 0, coOrdPort);
            try {
                new ForwardTask().executeOnExecutor(mExecutor, message).get();

                if (message.getKey() == null) {
                    // coordinator did not ACK, then send it to the node after that
                    try {
                        Map.Entry<String, Integer> firstSuccessor = mNodeMap.higherEntry(HashUtility.genHash(Integer.toString(coOrdPort/2)));
                        if (firstSuccessor == null) {
                            firstSuccessor = mNodeMap.firstEntry();
                        }

                        message.setType(Message.Type.SUB_WRITE);
                        message.setKey(key);
                        message.setmPort(firstSuccessor.getValue());

                        Log.v(TAG, "Forwarding to first successor of coordinator " + firstSuccessor.getValue());
                        new ForwardTask().executeOnExecutor(mExecutor, message).get();
                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "onCreate: SHA-1 not supported");
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            // store locally
            insertLocal(key, val);

            // replicate twice
            replicate(key, val);
        }

		return uri;
	}

    public void insertLocal(String key, String val) {
        int version = 0;

        // read the current version
        try (FileInputStream fis = getContext().openFileInput(key);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
            br.readLine(); // skip value
            String ver = br.readLine();
            if (ver != null) {
                version = Integer.parseInt(ver);
            }
        } catch (FileNotFoundException fnf) {
            // do nothing, first insert
        } catch (IOException ioe) {
            Log.e(TAG, "Error reading from file - " + key);
        }

        try (FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE)) {
            fos.write(val.getBytes());
            fos.write('\n');
            ++version;
            fos.write(Integer.toString(version).getBytes());
            fos.write('\n');
            fos.flush();
        } catch (FileNotFoundException fnf) {
            Log.e(TAG, "File not found - " + key);
        } catch (IOException ioe) {
            Log.e(TAG, "Error writing to file - " + key);
        }
        Log.v(TAG, "insert local key: " + key + " value: " + val + " version: " +version);
    }

    public void replicate(String key, String val) {
        Message message = new Message(Message.Type.REPL_WRITE, key, val, 0, mPort);
        try {
            new ReplicationTask().executeOnExecutor(mExecutor, message).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Replicate SUB_WRITE
     *
     * @param key
     * @param val
     */
    public void replicateOnce(String key, String val) {
        Log.v(TAG, "replicateOnce successor: " + preflist.get(0));
        Message message = new Message(Message.Type.REPL_WRITE, key, val, 0, preflist.get(0));
        try {
            new ForwardTask().executeOnExecutor(mExecutor, message).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		if ("@".equals(selection)) {
            return queryAllLocal();
        } else if ("*".equals(selection)) {
            return queryAll();
        } else {
            int coOrdPort = getCoOrdinator(selection);
            if (coOrdPort != mPort) {
                Log.v(TAG, "forwarding " + selection + " to co-ordinator " + coOrdPort);

                // send it to the right node
                Message message = new Message(Message.Type.READ, selection, null, 0, coOrdPort);
                try {
                    new ForwardTask().executeOnExecutor(mExecutor, message).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                Log.v(TAG, "Forwarding done got back " + message);

                if (message.getKey() == null) {
                    // co-ordinator is down
                    // read from the successor of the co-ordinator
                    message.setKey(selection);
                    message.setType(Message.Type.REPL_READ);

                    try {
                        String coOrdHash = HashUtility.genHash(Integer.toString(coOrdPort / 2));
                        String successor = mNodeMap.higherKey(coOrdHash);
                        if (successor == null) {
                            successor = mNodeMap.firstKey();
                        }
                        int successorPort = mNodeMap.get(successor);
                        message.setmPort(successorPort);

                        Log.v(TAG, "Co-ordinator " + coOrdPort + " is down. Reading from  " + successorPort);
                        try {
                            new ForwardTask().executeOnExecutor(mExecutor, message).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "onCreate: SHA-1 not supported");
                    }
                }

                MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
                cursor.addRow(new String[]{message.getKey(), message.getValue()});
                return cursor;
            } else {
                return querySuccessors(selection);
            }
        }
	}

    private Cursor queryAllLocal() {
        // TODO: change to use queryAllWithVersion
        Log.v(TAG, "queryAllLocal");
        MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
        String[] allKeys = getContext().fileList();
        for (String k : allKeys) {
            addValueToCursor(k, cursor);
        }
        return cursor;
    }

    public List<Map.Entry<String, String>> queryAllWithVersion() {
        List<Map.Entry<String, String>> result = new ArrayList<Map.Entry<String, String>>();
        String[] allKeys = getContext().fileList();
        for (String k : allKeys) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(k)))) {
                String value = br.readLine();
                String version = br.readLine();
                Map.Entry<String, String> entry = new AbstractMap.SimpleEntry<String, String>(k, value + "," + version);
                result.add(entry);
            } catch (IOException ioe) {
                Log.v(TAG, "query local key - " + k + "not found");
            }
        }
        return result;
    }

    private void addValueToCursor(String key, MatrixCursor cursor) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(key)))) {
            String value = br.readLine();
            cursor.addRow(new String[]{key, value});
        } catch (IOException ioe) {
            Log.v(TAG, "query local key - " + key + "not found");
        }
    }

    public String[] queryLocal(String key) {
        Log.v(TAG, "queryLocal " + key);
        // get the local value
        int version;
        String value = "";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(key)))) {
            value = br.readLine();
            version = Integer.parseInt(br.readLine());
            Log.v(TAG, "queryLocal " + key + " value " + value + " version " + version);
        } catch (IOException ioe) {
            Log.v(TAG, "queryLocal local key - " + key + " not found");
            return null;
        }

        return new String[]{value, Integer.toString(version)};
    }

    private Cursor queryAll() {
        Log.v(TAG, "queryAll");
        Message message = new Message(Message.Type.READ_ALL, "*", null, 0, mPort);
        try {
            new QueryAllTask().executeOnExecutor(mExecutor, message).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
        for (Map.Entry<String, String> entry : message.getResult()) {
            cursor.addRow(new String[] {entry.getKey(), entry.getValue()});
        }
        return cursor;
    }

    private void sync(int successorPort, int predPort) {
        Log.v(TAG, "sync");
        //deleteLocal("*");
        Message message1 = new Message(Message.Type.READ_ALL, "*", null, 0, successorPort);
        Message message2 = new Message(Message.Type.READ_ALL, "*", null, 0, predPort);
        try {
            new SyncTask().executeOnExecutor(mExecutor, message1, message2).get();

            for (Map.Entry<String, String> entry : message1.getResult()) {
                insertLocal(entry.getKey(), entry.getValue());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private Cursor querySuccessors(String key){
        MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
        String[] result = queryLocal(key);
        if (result != null) {
            Log.v(TAG, "querySuccessors: local key " + key + " value " + result[0] + " version " + result[1]);
            Message message = new Message(Message.Type.REPL_READ, key, result[0], Integer.parseInt(result[1]), mPort);
            try {
                new QueryTask().executeOnExecutor(mExecutor, message).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            cursor.addRow(new String[]{message.getKey(), message.getValue()});
        } else {
            Log.v(TAG, "querySuccessors: key " + key + " not found");
        }
        return cursor;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
        // not used
		return 0;
	}

    /**
     * AsyncTask to forward the key-val to co-ordinator
     */
    private class ForwardTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), message.getmPort());
                 BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                 BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ){
                // pass the key-val to co-ordinator
                String msgToSend = message.toString();
                bw.write(msgToSend + "\n");
                bw.flush();
                Log.v(TAG, "forwarded " + msgToSend + " to " + message.getmPort());

                String line = br.readLine();
                Log.v(TAG, "ForwardTask received " + line);
                if (line != null && line.length() > 1) {
                    Message msg = new Message(line);
                    message.setKey(msg.getKey());
                    message.setValue(msg.getValue());
                } else {
                    // notify main thread of co-ordinator ACK failure
                    message.setKey(null);
                }
            } catch (IOException ioe) {
                Log.e(TAG, "Error forwarding");
                ioe.printStackTrace();
            }

            return null;
        }
    }

    /**
     * AsyncTask to replicate the key-val
     */
    private class ReplicationTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            for (int port : preflist) {
                try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                     BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
                ){
                    String msgToSend = message.toString();
                    bw.write(msgToSend + "\n");
                    bw.flush();
                    Log.v(TAG, "replication message sent " + msgToSend + " to " + port);
                } catch (IOException ioe) {
                    Log.e(TAG, "Error sending message to " + port);
                    ioe.printStackTrace();
                }
            }

            return null;
        }
    }

    /**
     * AsyncTask to delete the key-val
     */
    private class DeleteTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            for (int port : preflist) {
                try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                     BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
                ){
                    String msgToSend = message.toString();
                    bw.write(msgToSend + "\n");
                    bw.flush();
                    Log.v(TAG, "delete message sent " + msgToSend + " to " + port);
                } catch (IOException ioe) {
                    Log.e(TAG, "Error sending message to " + port);
                    ioe.printStackTrace();
                }
            }

            return null;
        }
    }

    /**
     * AsyncTask to delete everything
     */
    private class DeleteAllTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            for (int port : REMOTE_PORTS) {
                if (port != mPort) {
                    try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                         BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
                    ) {
                        String msgToSend = message.toString();
                        bw.write(msgToSend + "\n");
                        bw.flush();
                        Log.v(TAG, "delete message sent " + msgToSend);
                    } catch (IOException ioe) {
                        Log.e(TAG, "Error sending message to " + port);
                        ioe.printStackTrace();
                    }
                }
            }

            return null;
        }
    }

    /**
     * AsyncTask to pass the query message to successor
     */
    private class QueryTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            for (int port : preflist) {
                try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                     BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                     BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                ){
                    String msgToSend = message.toString();
                    bw.write(msgToSend + "\n");
                    bw.flush();
                    Log.v(TAG, "query message sent " + msgToSend + " to " + port);

                    String recvLine = br.readLine();
                    Log.v(TAG, "message received " + recvLine);
                    if (recvLine != null && recvLine.length() > 0) {
                        Message recvMsg = new Message(recvLine);
                        if (recvMsg.getVersion() > message.getVersion()) {
                            message.setValue(recvMsg.getValue());
                            message.setVersion(recvMsg.getVersion());
                        }
                    }
                } catch (IOException ioe) {
                    Log.e(TAG, "Error sending message to " + port);
                    ioe.printStackTrace();
                }
            }

            return null;
        }
    }

    /**
     * AsyncTask to query everything
     */
    private class QueryAllTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            Map<String, String> result = new HashMap<>();

            for (Map.Entry<String, String> entry : queryAllWithVersion()) {
                result.put(entry.getKey(), entry.getValue());
            }

            for (int port : REMOTE_PORTS) {
                if (port != mPort) {
                    try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                         BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                         BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                    ) {
                        String msgToSend = message.toString();
                        bw.write(msgToSend + "\n");
                        bw.flush();
                        Log.v(TAG, "query message sent " + msgToSend);

                        String line = br.readLine();
                        Log.v(TAG, "QueryAllTask response " + line + " from " + port);
                        if(line != null && line.length() > 0) {
                            Message respMsg = new Message(line);

                            for (Map.Entry<String, String> entry : respMsg.getResult()) {
                                String key = entry.getKey();
                                int version = Integer.parseInt(entry.getValue().split(",")[1]);

                                String existingVal = result.get(key);
                                if (existingVal != null) {
                                    int existingVer = Integer.parseInt(existingVal.substring(existingVal.indexOf(',') + 1));
                                    if (existingVer < version) {
                                        result.put(key, entry.getValue());
                                    }
                                } else {
                                    result.put(key, entry.getValue());
                                }
                            }
                        }
                    } catch (IOException ioe) {
                        Log.e(TAG, "Error sending message to " + port);
                        ioe.printStackTrace();
                    }
                }
            }

            List<Map.Entry<String, String>> output = new ArrayList<>(result.size());
            for (Map.Entry<String, String> entry : result.entrySet()) {
                output.add(new AbstractMap.SimpleEntry<String, String>(entry.getKey(), entry.getValue().substring(0, entry.getValue().indexOf(","))));
            }
            message.setResult(output);

            return null;
        }
    }

    /**
     * AsyncTask to sync from successor
     */
    private class SyncTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {

            Map<String, String> result = new HashMap<>();

            for (Map.Entry<String, String> entry : queryAllWithVersion()) {
                result.put(entry.getKey(), entry.getValue());
            }

            for (Message message : msgs) {
                try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), message.getmPort());
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                ) {
                    String msgToSend = message.toString();
                    bw.write(msgToSend + "\n");
                    bw.flush();
                    Log.v(TAG, "sync message sent " + msgToSend);

                    String line = br.readLine();
                    Log.v(TAG, "SyncTask response " + line + " from " + message.getmPort());
                    if(line != null && line.length() > 0) {
                        Message respMsg = new Message(line);

                        for (Map.Entry<String, String> entry : respMsg.getResult()) {
                            String key = entry.getKey();
                            int version = Integer.parseInt(entry.getValue().split(",")[1]);

                            // store, only if the key is replicated in this partition or belongs to partition
                            int partition = getCoOrdinator(key);
                            if (partition == mPort || prevNodes.contains(partition)) {
                                String existingVal = result.get(key);
                                if (existingVal != null) {
                                    int existingVer = Integer.parseInt(existingVal.substring(existingVal.indexOf(',') + 1));
                                    if (existingVer < version) {
                                        result.put(key, entry.getValue());
                                    }
                                } else {
                                    result.put(key, entry.getValue());
                                }
                            } else {
                                Log.v(TAG, "dropped key " + key + " from " + message.getmPort() + ", belongs to " + partition);
                            }
                        }
                    }
                } catch (IOException ioe) {
                    Log.e(TAG, "Error sending message to " + message.getmPort());
                    ioe.printStackTrace();
                }
            }

            List<Map.Entry<String, String>> output = new ArrayList<>(result.size());
            for (Map.Entry<String, String> entry : result.entrySet()) {
                output.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().substring(0, entry.getValue().indexOf(","))));
            }

            msgs[0].setResult(output);

            return null;
        }
    }
}
