import java.io.*;
import java.net.*;
import java.util.*;

// Class representing a node in the distributed database
class DatabaseNode {
    // Map to store key-value pairs
    private Data data;
    // Set to store connections to other nodes
    private final List<IPv4Address> connections;
    // Server socket to listen for client connections
    private int serverPort;
    private final ServerSocket socketTCP;
    private boolean active;

    public DatabaseNode(int port, int key, int value, List<IPv4Address> connections) throws IOException {
        this.serverPort = port;
        this.active = true;
        this.connections = connections;

        data = new Data(key, value);
        socketTCP = new ServerSocket(port);

        if(serverPort == 0) serverPort = socketTCP.getLocalPort();

        listenForConnections();

        printMessage(String.valueOf(serverPort), "Created new node");
        for(IPv4Address a: connections){
            String res = sendNodeRequest(a, "add-connection localhost:"+serverPort);
            System.out.println(res);
        }
    }

    private void listenForConnections() {
        // Start listening for client connections in a new thread
        new Thread(() -> {
            while (active) {
                try {
                    Socket newClientSocket = socketTCP.accept();
                    handleGuests(newClientSocket);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    private void handleGuests(Socket socket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line = in.readLine();

        String[] msg = line.split("//");

        if(msg[0].equalsIgnoreCase("node")) handleNode(socket, msg[1]);
        else handleClient(socket, line);

    }

    // Handles a client connection
    private void handleClient(Socket socket, String line) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        printMessage(String.valueOf(serverPort), "Client ["+socket.getPort()+"] connected");


        String[] parts = line.split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];

        printMessage(String.valueOf(serverPort), "Client ["+socket.getPort()+"] requested: "+operation);

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

        socket.close();
        printMessage(String.valueOf(serverPort), "Client ["+socket.getPort()+"] disconnected");

    }
    // Handles a node connection
    private void handleNode(Socket socket, String line) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        printMessage(String.valueOf(serverPort), "Node ["+socket.getPort()+"] connected and requested: "+line);

        String[] parts = line.split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];

        switch (operation){
            case "add-connection":
                args = parts[1].split(":");

                IPv4Address toAdd = new IPv4Address(args[0], Integer.parseInt(args[1]));
                System.out.println(toAdd);
                connections.add(toAdd);
                out.println("Added: "+ toAdd);
        }

    }

    public String setValue(int key, int value){
        if(key == data.getKey()){
            data.setValue(value);
            return "OK";
        }
        return sendNodeRequest(new IPv4Address(connections.get(0).getIp(), connections.get(0).getPort()), "Hello test 1");
    }

    public String getValue(int key){
        if(key == data.getKey()){
            return data.getKey()+":"+data.getValue();
        }
        return "ERROR";
    }

    public String findKey(int key){
        if(key == data.getKey()){
            return socketTCP.getLocalSocketAddress()+":"+socketTCP.getLocalPort();
        }
        return "ERROR";
    }

    // Sends a request to all connections and returns the responses
    private String sendNodeRequest(IPv4Address address ,String request){
        printMessage(String.valueOf(serverPort), "Sending \""+request+"\" request to: "+address.getPort());
        String res;
        Socket socket;
        try{
            socket = new Socket(address.getIp(), address.getPort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println("node//"+request);
            // Read and print out the response
            res = in.readLine();
            return res;
        } catch (IOException e) {
            return "ERROR";
        }
    }

    public static void printMessage(String sender, String msg){
        System.out.println("[LOG from: "+sender+"]: "+msg);
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
        new DatabaseNode(tcpPort, key, value, connections);
    }

}

