import java.io.*;
import java.net.*;
import java.util.*;

// Class representing a node in the distributed database
class DatabaseNode {
    // Map to store key-value pairs
    private Map<Integer, Integer> data;
    // Set to store connections to other nodes
    private Set<Socket> connections;
    // Server socket to listen for client connections
    private ServerSocket serverSocket;
    private int tcpPort;
    private boolean active;

    public DatabaseNode(int tcpPort, int key, int value, List<IPv4Address> addresses) throws IOException {
        this.tcpPort = tcpPort;
        this.active = true;
        data = new HashMap<>();
        data.put(key, value);
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

        printMessage(String.valueOf(tcpPort), "Created new node");
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

    // Handles a client connection
    private void handleClient(Socket socket) throws IOException {
        printMessage(String.valueOf(tcpPort), "Client ["+socket.getPort()+"] connected");


        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream());

        String line = in.readLine();
        String[] parts = line.split(" ");
        String operation = parts[0];

        printMessage(String.valueOf(tcpPort), "Client ["+socket.getPort()+"] requested: "+operation);

        switch (operation){
            case "set-value":
                int setKey = Integer.parseInt(parts[1]);
                int setValue = Integer.parseInt(parts[2]);
                data.put(setKey, setValue);
                out.println("OK");
                break;
            case "get-value":
                int getKey = Integer.parseInt(parts[1]);
                if (data.containsKey(getKey)) {
                    out.println(data.get(getKey));
                } else {
                    out.println("ERROR");
                }
                break;
            case "terminate":
                active = false;

        }

        out.flush();
        socket.close();
        printMessage(String.valueOf(tcpPort), "Client ["+socket.getPort()+"] disconnected");

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

