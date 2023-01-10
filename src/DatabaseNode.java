import java.io.*;
import java.net.*;
import java.util.*;

// Class representing a node in the distributed database
class DatabaseNode {
    public static String MESSAGE_OK = "OK";
    public static String MESSAGE_ERROR = "ERROR";
    public static String MESSAGE_UNKNOWN_COMMAND = "UNKNOWN COMMAND";

    // The IP address and port of this node
    private IPv4Address serverAddress;

    // The data stored by this node
    private Data data;

    // Flag indicating whether this node is active
    private boolean active;

    // List of connections to other nodes in the database
    private final List<IPv4Address> connections;

    // List of request IDs processed by this node
    private List<Integer> idLog;

    // ServerSocket for accepting client connections over TCP
    private final ServerSocket socketTCP;

    public DatabaseNode(int port, int key, int value, List<IPv4Address> connections) throws IOException {
        // Initialize fields
        this.serverAddress = new IPv4Address("localhost", port);
        this.data = new Data(key, value);
        this.active = true;
        this.connections = connections;
        this.idLog = new ArrayList<>();
        this.socketTCP = new ServerSocket(port);

        // If the port was not specified, use the port assigned by the operating system
        if(serverAddress.getPort() == 0) serverAddress.setPort(socketTCP.getLocalPort());

        // Start listening for client connections in a new thread
        listenForConnections();

        printMessage(String.valueOf(serverAddress.getPort()), "Created new node");

        // Send a message to each of the specified connections to add this node to their list of connections
        for(IPv4Address a: connections){
            sendNodeRequest(a, Collections.singletonList(serverAddress), "add-connection",-1);
        }
    }

    /**
     * Starts a new thread to listen for incoming client and node connections on the specified port.
     */
    private void listenForConnections() {
        new Thread(() -> {
            while (active) {
                try {
                    Socket newClientSocket = socketTCP.accept();
                    new Thread(()->{
                        try {
                            handleGuests(newClientSocket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }).start();


                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    /**
     * Handles incoming connections, determining if the connection is from a client or another node, and routing it to the appropriate handling method.
     */
    private void handleGuests(Socket socket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line = in.readLine();

        String[] msg = line.split("//");

        if(msg[0].equalsIgnoreCase("node")) handleNode(socket, msg);
        else handleClient(socket, line);

    }

    /**
     * Handles incoming connections from a client, processing the client's requested operation and sending a response.
     * @param line client's requested message
     */
    private void handleClient(Socket socket, String line) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        printMessage(String.valueOf(serverAddress.getPort()), "Client ["+socket.getPort()+"] connected");


        String[] parts = line.split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];
        int requestId = (int) (new Date().getTime()/100);

        List<IPv4Address> trace = new ArrayList<>();
        trace.add(serverAddress);

        printMessage(String.valueOf(serverAddress.getPort()), "Client ["+socket.getPort()+"] requested: "+operation+" with id:"+requestId);


        switch (operation){
            case "set-value":
                args = parts[1].split(":");
                res = setValue(Integer.parseInt(args[0]), Integer.parseInt(args[1]),requestId, null, trace);
                out.println(res);
                break;
            case "get-value":
                res = getValue(Integer.parseInt(parts[1]), requestId, null, trace);
                out.println(res);
                break;
            case "find-key":
                res = findKey(Integer.parseInt(parts[1]), requestId, null, trace);
                out.println(res);
                break;
            case "get-max":
                res =  getMax(Integer.MIN_VALUE,requestId,null, trace);
                out.println(res);
                break;
            case "get-min":
                res = getMin(Integer.MAX_VALUE,requestId,null, trace);
                out.println(res);
                break;
            case "new-record":
                args = parts[1].split(":");
                data = new Data(Integer.parseInt(args[0]),Integer.parseInt(args[1]));
                res = MESSAGE_OK;
                out.println(res);
                break;
            case "terminate":
                res = terminate(requestId);
                out.println(res);
                break;
            case "get-cons":
                res = connections.toString();
                out.println(res);
                break;
            default:
                res = MESSAGE_UNKNOWN_COMMAND;
                out.println(res);
                break;

        }

        printMessage(String.valueOf(serverAddress.getPort()), "Sent message to Client ["+socket.getPort()+"]: "+res);


        socket.close();
        printMessage(String.valueOf(serverAddress.getPort()), "Client ["+socket.getPort()+"] disconnected");

    }

    /**
     * Handles incoming connections from another node in the network.
     * @param msg array of parts of message from the other nodes
     */
    private void handleNode(Socket socket, String[] msg) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);


        IPv4Address sender = new IPv4Address(msg[msg.length-1]);
        List<IPv4Address> trace = getTrace(msg[msg.length-2]);
        trace.add(serverAddress);

        int requestId = Integer.parseInt(msg[2]);
        String[] parts = msg[1].split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];

        printMessage(String.valueOf(serverAddress.getPort()), "Node ["+sender.getPort()+"] connected and requested: "+msg[1]+" with id: "+requestId );


        if(idLog.contains(requestId)){
            printMessage(String.valueOf(serverAddress.getPort()), "Request: "+msg[1]+" with id: "+requestId+" have been used already");
            out.println(MESSAGE_ERROR);
            socket.close();
            return;
        }

        if(requestId != -1) idLog.add(requestId);

        switch (operation){
            case "add-connection":
                if(!connections.toString().contains(sender.toString())){
                    connections.add(sender);
                    res = "Added: "+ sender;
                    out.println(res);
                }
                else res = "Already in connections list";
                break;
            case "set-value":
                args = parts[1].split(":");
                res = setValue(Integer.parseInt(args[0]), Integer.parseInt(args[1]),requestId, sender,trace);
                out.println(res);
                break;
            case "get-value":
                res = getValue(Integer.parseInt(parts[1]),requestId, sender,trace);
                out.println(res);
                break;
            case "get-max":
                res = getMax(Integer.parseInt(parts[1]),requestId, sender,trace);
                out.println(res);
                break;
            case "get-min":
                res = getMin(Integer.parseInt(parts[1]),requestId, sender,trace);
                out.println(res);
                break;
            case "find-key":
                res = findKey(Integer.parseInt(parts[1]), requestId, sender,trace);
                out.println(res);
                break;
            case "terminate":
                for(int i = 0;i < connections.size() ;i++){
                    if(connections.get(i).equals(sender)){
                        connections.remove(i);
                        i--;
                    }
                }
                printMessage(String.valueOf(serverAddress.getPort()), "Connections left:"+ connections);
                res = MESSAGE_OK;
                out.println(res);
                break;
            default:
                out.println(Arrays.toString(msg));
                break;

        }

        printMessage(String.valueOf(serverAddress.getPort()), "Sent message to Node ["+sender.getPort()+"]: "+res);


        socket.close();
    }

    public String setValue(int key, int value, int id, IPv4Address sender, List<IPv4Address> trace){
        if(key == data.getKey()){
            data.setValue(value);
            return MESSAGE_OK;
        }
        List<IPv4Address> targets = getTargets(sender);
        String res = MESSAGE_ERROR;
        for(IPv4Address a: targets){
            if(!trace.toString().contains(a.toString())){
                res = sendNodeRequest(a, trace,"set-value "+key+":"+value,id);
                res = getMessageFromResponse(res);
                if(!res.equals(MESSAGE_ERROR)) return res;
            }
        }
        return res;
    }

    public String getValue(int key, int id, IPv4Address sender, List<IPv4Address> trace){
        if(key == data.getKey()){
            return data.getKey()+":"+data.getValue();
        }
        List<IPv4Address> targets = getTargets(sender);

        String res = MESSAGE_ERROR;
        for(IPv4Address a: targets){
            if(!trace.toString().contains(a.toString())){
                res = sendNodeRequest(a, trace,"get-value "+key,id);
                if(!res.equals(MESSAGE_ERROR)) return res;
            }

        }
        return res;
    }

    public String findKey(int key, int id, IPv4Address sender, List<IPv4Address> trace){
        if(key == data.getKey()){
            return serverAddress.toString();
        }
        List<IPv4Address> targets = getTargets(sender);
        String res = MESSAGE_ERROR;
        for(IPv4Address a: targets){
            if(!trace.toString().contains(a.toString())){
                res = sendNodeRequest(a, trace, "find-key "+key,id);
                if(!res.equals(MESSAGE_ERROR)) return res;
            }

        }
        return res;
    }

    public String getMax(int value, int id, IPv4Address sender, List<IPv4Address> trace){

        List<IPv4Address> targets = getTargets(sender);
        List<String> responses = new ArrayList<>();
        String res;
        for(IPv4Address a: targets){
            if(!trace.toString().contains(a.toString())){
                res = sendNodeRequest(a, trace, "get-max "+value,id);
                if(!res.equals(MESSAGE_ERROR)) responses.add(res);
            }

        }
        res = data.toString();
        responses.add(res);
        for (String s: responses){
            String[] args = s.split(":");
            int newValue = Integer.parseInt(args[1]);
            if(newValue > value){
                value = newValue;
                res = s;
            }
        }
        return res;
    }

    public String getMin(int value, int id, IPv4Address sender, List<IPv4Address> trace){

        List<IPv4Address> targets = getTargets(sender);
        List<String> responses = new ArrayList<>();
        String res;
        for(IPv4Address a: targets){
            if(!trace.toString().contains(a.toString())){
                res = sendNodeRequest(a, trace,"get-min "+value,id);
                if(!res.equals(MESSAGE_ERROR)) responses.add(res);
            }
        }
        res = data.toString();
        responses.add(res);
        for (String s: responses){
            String[] args = s.split(":");
            int newValue = Integer.parseInt(args[1]);
            if(newValue < value){
                value = newValue;
                res = s;
            }
        }
        return res;
    }

    public String terminate(int id){
        active = false;
        for(IPv4Address a: connections){
            sendNodeRequest(a, Collections.singletonList(serverAddress), "terminate",id);
        }
        return MESSAGE_OK;
    }

    /**
     * Creates message from params and sends it to target node
     * @return response from target
     */
    private String sendNodeRequest(IPv4Address address,List<IPv4Address> owner ,String request, int id){
        printMessage(String.valueOf(serverAddress.getPort()), "Sending \""+request+"\" request to: "+address.getPort());
        String res;
        Socket socket;
        try{
            socket = new Socket(address.getIp(), address.getPort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println("node//"+request+"//"+id+"//"+owner+"//"+serverAddress);
            // Read and print out the response
            res = in.readLine();
            return res;
        } catch (IOException e) {
            return MESSAGE_ERROR;
        }
    }

    /**
     * Reads request received from a node
     * @param res full response from a node
     * @return String with requested operation
     */
    private String getMessageFromResponse(String res){
        if(res.contains("node//")){
            String[] parts = res.split("//");
            return parts[1];
        }
        else return res;
    }

    /**
     * Converts String to List of IPv4Address
     * @return List of IPv4Address
     */
    private List<IPv4Address> getTrace(String s){
        s = s.replace("[", "").replace("]", "");
        String[] parts = s.split(", ");
        List<IPv4Address> addresses = new ArrayList<>();
        for(String a: parts) addresses.add(new IPv4Address(a));
        return addresses;
    }

    /**
     * Finding targets from registered connections
     * @param sender connection to exclude
     * @return List of IPv4Address with targets that exclude sender
     */
    public List<IPv4Address> getTargets(IPv4Address sender){
        List<IPv4Address> list = new ArrayList<>(connections);
        if(sender != null){
            for(int i = 0;i < list.size() ;i++){
                if(list.get(i).equals(sender)){
                    list.remove(i);
                    i--;
                }

            }
        }
        return list;

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

