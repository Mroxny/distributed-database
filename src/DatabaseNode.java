import java.io.*;
import java.net.*;
import java.util.*;

// Class representing a node in the distributed database
class DatabaseNode {
    // Map to store key-value pairs
    private Data data;
    // Set to store connections to other nodes
    private Set<IPv4Address> connections;
    // Server socket to listen for client connections
    private int serverPort;
    private ServerSocket socketTCP;
    private DatagramSocket socketUDP;
    private boolean active;

    public DatabaseNode(int port, int key, int value, List<IPv4Address> addresses) throws IOException {
        this.serverPort = port;
        this.active = true;
        data = new Data(key, value);
        connections = new HashSet<>();
        connections.addAll(addresses);
        socketTCP = new ServerSocket(port);
        socketUDP = new DatagramSocket(port);

        if(serverPort == 0) serverPort = socketTCP.getLocalPort();

        listenForConnections();

        printMessage(String.valueOf(serverPort), "Created new node");
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

    public String setValue(int key, int value){
        if(key == data.getKey()){
            data.setValue(value);
            return "OK";
        }
        sendNodeRequest("Hello");
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
            return socketTCP.getLocalSocketAddress()+":"+socketTCP.getLocalPort();
        }
        return "ERROR";
    }


    // Handles a client connection
    private void handleClient(Socket socket) throws IOException {
        printMessage(String.valueOf(serverPort), "Client ["+socket.getPort()+"] connected");


        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream());

        String line = in.readLine();
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

        out.flush();
        socket.close();
        printMessage(String.valueOf(serverPort), "Client ["+socket.getPort()+"] disconnected");

    }
    // Handles a node connection
    private void handleNode(DatagramPacket pocket){
        printMessage(String.valueOf(serverPort), "Node ["+pocket.getPort()+"] connected");
        printMessage(String.valueOf(serverPort), "Node ["+pocket.getPort()+"] said: "+ new String(pocket.getData()));


    }

    // Sends a request to all connections and returns the responses
    private String sendNodeRequest(String request){
        String res = "ERROR";

        for (IPv4Address address : connections) {
            printMessage(String.valueOf(serverPort), "Sending request to: "+address.getPort());


            try{
                byte[] message = request.getBytes();

                // Create a DatagramPacket with the message and the address of the recipient
                InetAddress ip = InetAddress.getByName(address.getIp());
                DatagramPacket packet = new DatagramPacket(message, message.length, ip, address.getPort());

                // Send the packet
                socketUDP.send(packet);

                byte[] buffer = new byte[1024];

                // Create a DatagramPacket to receive the message
                packet = new DatagramPacket(buffer, buffer.length);

                // Wait for a message to arrive
                socketUDP.receive(packet);

                message = packet.getData();
                res = new String(message);
                if(!res.equals("ERROR")) return res;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
        return res;
    }

    private void listenForConnections(){
        // Start listening for client connections in a new thread
        new Thread(() -> {
            while (active) {
                try {
                    Socket newClientSocket = socketTCP.accept();
                    handleClient(newClientSocket);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {

            byte[] buffer = new byte[1024];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while (active) {
                try {
                    socketUDP.receive(packet);
                    handleNode(packet);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }


    public static void printMessage(String sender, String msg){
        System.out.println("[LOG from: "+sender+"]: "+msg);
    }

}

