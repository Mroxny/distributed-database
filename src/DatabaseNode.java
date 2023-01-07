import java.io.*;
import java.net.*;
import java.util.*;

// Class representing a node in the distributed database
class DatabaseNode {
    private IPv4Address serverAddress;
    private Data data;
    private boolean active;
    private final List<IPv4Address> connections;
    private List<Integer> idLog;
    private final ServerSocket socketTCP;


    public DatabaseNode(int port, int key, int value, List<IPv4Address> connections) throws IOException {
        this.serverAddress = new IPv4Address("localhost", port);
        this.data = new Data(key, value);
        this.active = true;
        this.connections = connections;
        this.idLog = new ArrayList<>();
        this.socketTCP = new ServerSocket(port);

        if(serverAddress.getPort() == 0) serverAddress.setPort(socketTCP.getLocalPort());

        listenForConnections();

        printMessage(String.valueOf(serverAddress.getPort()), "Created new node");

        for(IPv4Address a: connections){
            sendNodeRequest(a, "add-connection");
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

        if(msg[0].equalsIgnoreCase("node")) handleNode(socket, msg);
        else handleClient(socket, line);

    }

    // Handles a client connection
    private void handleClient(Socket socket, String line) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        printMessage(String.valueOf(serverAddress.getPort()), "Client ["+socket.getPort()+"] connected");


        String[] parts = line.split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];

        printMessage(String.valueOf(serverAddress.getPort()), "Client ["+socket.getPort()+"] requested: "+operation);

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
        printMessage(String.valueOf(serverAddress.getPort()), "Client ["+socket.getPort()+"] disconnected");

    }
    // Handles a node connection
    private void handleNode(Socket socket, String[] msg) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);


        IPv4Address sender = new IPv4Address(msg[msg.length-1]);
        String[] parts = msg[0].split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];

        printMessage(String.valueOf(serverAddress.getPort()), "Node ["+sender.getPort()+"] connected and requested: "+msg[1]);



        switch (operation){
            case "add-connection":

                System.out.println(sender);
                connections.add(sender);
                out.println("Added: "+ sender);
                break;
            case "terminate":


        }

    }

    public String setValue(int key, int value){
        int id = (int) (new Date().getTime()/1000);
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
    private String sendNodeRequest(IPv4Address address ,String request, int id){
        printMessage(String.valueOf(serverAddress.getPort()), "Sending \""+request+"\" request to: "+address.getPort());
        String res;
        Socket socket;
        try{
            socket = new Socket(address.getIp(), address.getPort());
            System.out.println(socket.getPort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println("node//"+request+"//"+id+"//"+serverAddress);
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

