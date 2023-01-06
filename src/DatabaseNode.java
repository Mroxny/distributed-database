import java.io.*;
import java.net.*;
import java.util.*;

// Class representing a node in the distributed database
class DatabaseNode {
    // Map to store key-value pairs
    private Data data;
    // Set to store connections to other nodes
    private Set<Socket> connections;
    // Server socket to listen for client connections
    private ServerSocket serverSocket;
    private boolean active;

    public DatabaseNode(int tcpPort, int key, int value, List<IPv4Address> addresses) throws IOException {
        this.active = true;
        data = new Data(key, value);
        connections = new HashSet<>();
        serverSocket = new ServerSocket(tcpPort);

        // Connect to other nodes
        for (IPv4Address address : addresses) {
            Socket socket = new Socket(address.getIp(), address.getPort());
            connections.add(socket);
        }

        // Start listening for client connections in a new thread
        new Thread(() -> {

            while (active) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    handleClient(clientSocket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        printMessage(String.valueOf(serverSocket.getLocalPort()), "Created new node");
    }

    public static void main(String[] args) throws IOException {

        int tcpPort = 0;
        int key = 0;
        int value = 0;
        List<IPv4Address> connections = new ArrayList<>();


        for(int i=0; i<args.length; i++) {
            switch (args[i]) {
                case "-tcpport":
                    tcpPort = Integer.parseInt(args[++i]);
                    break;
                case "-record":
                    String[] mapArray = args[++i].split(":");
                    key = Integer.parseInt(mapArray[0]);
                    value = Integer.parseInt(mapArray[1]);
                    break;
                case "-connect":
                    String[] addressArray = args[++i].split(":");
                    connections.add(new IPv4Address(addressArray[0], Integer.parseInt(addressArray[1])));
                    break;
                default:
                    return;
            }
        }
        DatabaseNode node = new DatabaseNode(tcpPort, key, value, connections);
    }

    public String setValue(int key, int value){
        if(key == data.getKey()){
            data.setValue(value);
            return "OK";
        }
        return "ERROR";
    }

    public String getValue(int key){
        if(key == data.getKey()){
            return data.getKey()+":"+data.getValue();
        }
        return "ERROR";
    }

    public String findKey(int key){
        if(key == data.getKey()){
            return serverSocket.getLocalSocketAddress()+":"+serverSocket.getLocalPort();
        }
        return "ERROR";
    }

    // Handles a client connection
    private void handleClient(Socket socket) throws IOException {
        printMessage(String.valueOf(serverSocket.getLocalPort()), "Client ["+socket.getPort()+"] connected");


        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream());

        String line = in.readLine();
        String[] parts = line.split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];

        printMessage(String.valueOf(serverSocket.getLocalPort()), "Client ["+socket.getPort()+"] requested: "+operation);

        switch (operation){
            case "set-value":
                args = parts[1].split(":");
                res = setValue(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
                out.println(res);
                break;
            case "get-value":
                res = getValue(Integer.parseInt(parts[1]));
                out.println(res);
                break;
            case "find-key":
                res = findKey(Integer.parseInt(parts[1]));
                out.println(res);
                break;
            case "get-max":
                res = "MAX";
                out.println(res);
                break;
            case "get-min":
                res = "MIN";
                out.println(res);
                break;
            case "new-record":
                args = parts[1].split(":");
                data = new Data(Integer.parseInt(args[0]),Integer.parseInt(args[1]));
                res = "OK";
                out.println(res);
                break;
            case "terminate":
                active = false;

        }

        out.flush();
        socket.close();
        printMessage(String.valueOf(serverSocket.getLocalPort()), "Client ["+socket.getPort()+"] disconnected");

    }


    // Sends a request to all connections and returns the responses
    private List<String> sendRequest(String request) throws IOException {
        List<String> responses = new ArrayList<>();

        for (Socket socket : connections) {
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println(request);
            out.flush();
            responses.add(in.readLine());
        }

        return responses;
    }


    public static void printMessage(String sender, String msg){
        System.out.println("[LOG from: "+sender+"]: "+msg);
    }

}

