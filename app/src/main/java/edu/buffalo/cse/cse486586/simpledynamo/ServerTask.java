package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * Created by kishore on 3/18/16.
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    private static final String TAG = ServerTask.class.getSimpleName();

    private SimpleDynamoProvider mProvider;
    private int mPort; // the port of this node
    private String mNodeId; // hash of node port
    private MessageStore mStore;

    public ServerTask(int myPort, String nodeId, SimpleDynamoProvider provider, ContentResolver cr) {
        this.mPort = myPort;
        this.mNodeId = nodeId;
        this.mProvider = provider;
        this.mStore = new MessageStore(cr);
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        Log.v(TAG, "ServerTask started");

        ServerSocket serverSocket = sockets[0];
        while (!isCancelled()) {
            try {
                Socket clientSocket = serverSocket.accept();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                     BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))
                ) {
                    String line = br.readLine();
                    Log.v(TAG, "received message " + line);
                    Message msg = new Message(line);

                    switch (msg.getType()) {
                        case REPL_WRITE:
                            mProvider.insertLocal(msg.getKey(), msg.getValue());
                            break;
                        case REPL_READ:
                            handle_repl_read(msg.getKey(), bw);
                            break;
                        case DEL:
                            mProvider.deleteLocal(msg.getKey());
                            break;
                        case WRITE:
                            mStore.insert(msg.getKey(), msg.getValue());
                            break;
                        case READ:
                            handle_read(msg.getKey(), bw);
                            break;
                        case READ_ALL:
                            handle_read_all(bw);
                            break;
                        default:
                            Log.e(TAG, "invalid message " + msg);
                            break;
                    }
                } catch (IOException ioe) {
                    Log.e(TAG, "Error writing or reading to client socket");
                    ioe.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "Error while accepting the client connection");
            }
        }

        try {
            serverSocket.close();
        } catch (IOException ioe) {
            Log.e(TAG, "Could not close server socket");
        }

        return null;
    }

    private void handle_repl_read(String key, BufferedWriter bw) {
        String[] result = mProvider.queryLocal(key);
        if (result != null) {
            Message message = new Message(Message.Type.REPL_READ, key, result[0], Integer.parseInt(result[1]));
            try {
                String line = message.toString();
                bw.write(line);
                bw.write('\n');
                bw.flush();
                Log.v(TAG, "handle_repl_read: sent message " + line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Log.v(TAG, "handle_repl_read: key " + key + " not found");
        }
    }

    private void handle_read(String key, BufferedWriter bw) {
        List<Map.Entry<String, String>> result =  mStore.query(key);

        Log.v(TAG, "handle_read: after result " + result.get(0).getValue());
        try {
            Message msg = new Message(Message.Type.READ, key, result.get(0).getValue(), 0);
            String line = msg.toString();
            bw.write(line);
            bw.write('\n');
            bw.flush();
            Log.v(TAG, "handle_read: sent message " + line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handle_read_all(BufferedWriter bw) {
        Message respMsg = new Message(Message.Type.READ_ALL, "*", null, 0);
        respMsg.setResult(mProvider.queryAllWithVersion());
        String line = respMsg.toString();
        try {
            bw.write(line);
            bw.write('\n');
            bw.flush();
            Log.v(TAG, "handle_read_all: sent message " + line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
