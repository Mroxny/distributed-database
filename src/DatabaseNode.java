import java.io.*;
import java.net.*;
import java.util.*;

// Class representing a node in the distributed database
class DatabaseNode {
    public static String MESSAGE_OK = "OK";
    public static String MESSAGE_ERROR = "ERROR";
    public static String MESSAGE_UNKNOWN_COMMAND = "UNKNOWN COMMAND";



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
            sendNodeRequest(a, "add-connection",-1);
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
        int requestId = (int) (new Date().getTime()/1000);

        printMessage(String.valueOf(serverAddress.getPort()), "Client ["+socket.getPort()+"] requested: "+operation+" with id:"+requestId);


        switch (operation){
            case "set-value":
                args = parts[1].split(":");
                res = setValue(Integer.parseInt(args[0]), Integer.parseInt(args[1]),requestId, null);
                out.println(res);
                break;
            case "get-value":
                res = getValue(Integer.parseInt(parts[1]), requestId, null);
                out.println(res);
                break;
            case "find-key":
                res = findKey(Integer.parseInt(parts[1]), requestId, null);
                out.println(res);
                break;
            case "get-max":
                res =  getMax(Integer.MIN_VALUE,requestId,null);
                out.println(res);
                break;
            case "get-min":
                res = getMin(Integer.MAX_VALUE,requestId,null);
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
    // Handles a node connection
    private void handleNode(Socket socket, String[] msg) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);


        IPv4Address sender = new IPv4Address(msg[msg.length-1]);
        int requestId = Integer.parseInt(msg[2]);
        String[] parts = msg[1].split(" ");
        String operation = parts[0];
        String res = "";
        String[] args = new String[0];

        printMessage(String.valueOf(serverAddress.getPort()), "Node ["+sender.getPort()+"] connected and requested: "+msg[1]+" with id: "+requestId);

        if(idLog.contains(requestId)){
            printMessage(String.valueOf(serverAddress.getPort()), "Request: "+msg[1]+" with id: "+requestId+"have been used already");
            out.println(MESSAGE_ERROR);
            socket.close();
            return;
        }

        if(requestId != -1) idLog.add(requestId);

        switch (operation){
            case "add-connection":
                connections.add(sender);
                res = "Added: "+ sender;
                out.println(res);
                break;
            case "set-value":
                args = parts[1].split(":");
                res = setValue(Integer.parseInt(args[0]), Integer.parseInt(args[1]),requestId, sender);
                out.println(res);
                break;
            case "get-value":
                res = getValue(Integer.parseInt(parts[1]),requestId, sender);
                out.println(res);
                break;
            case "get-max":
                res = getMax(Integer.parseInt(parts[1]),requestId, sender);
                out.println(res);
                break;
            case "get-min":
                res = getMin(Integer.parseInt(parts[1]),requestId, sender);
                out.println(res);
                break;
            case "find-key":
                res = findKey(Integer.parseInt(parts[1]), requestId, sender);
                out.println(res);
                break;
            case "terminate":
                for(int i = 0;i < connections.size() ;i++){
                    if(connections.get(i).equals(sender)){
                        connections.remove(i);
                        break;
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

    public String setValue(int key, int value, int id, IPv4Address sender){
        if(key == data.getKey()){
            data.setValue(value);
            return MESSAGE_OK;
        }
        List<IPv4Address> targets = getTargets(sender);
        String res = MESSAGE_ERROR;
        for(IPv4Address a: targets){
            res = sendNodeRequest(a, "set-value "+key+":"+value,id);
            res = getMessageFromResponse(res);
            if(!res.equals(MESSAGE_ERROR)) return res;
        }
        return res;
    }

    public String getValue(int key, int id, IPv4Address sender){
        if(key == data.getKey()){
            return data.getKey()+":"+data.getValue();
        }
        List<IPv4Address> targets = getTargets(sender);
        String res = MESSAGE_ERROR;
        for(IPv4Address a: targets){
            res = sendNodeRequest(a, "get-value "+key,id);
            res = getMessageFromResponse(res);
            if(!res.equals(MESSAGE_ERROR)) return res;
        }
        return res;
    }

    public String findKey(int key, int id, IPv4Address sender){
        if(key == data.getKey()){
            return serverAddress.toString();
        }
        List<IPv4Address> targets = getTargets(sender);
        String res = MESSAGE_ERROR;
        for(IPv4Address a: targets){
            res = sendNodeRequest(a, "find-key "+key,id);
            res = getMessageFromResponse(res);
            if(!res.equals(MESSAGE_ERROR)) return res;
        }
        return res;
    }

    public String getMax(int value, int id, IPv4Address sender){

        List<IPv4Address> targets = getTargets(sender);
        List<String> responses = new ArrayList<>();
        String res;
        for(IPv4Address a: targets){
            res = sendNodeRequest(a, "get-max "+value,id);
            if(!res.equals(MESSAGE_ERROR)) responses.add(res);
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

    public String getMin(int value, int id, IPv4Address sender){

        List<IPv4Address> targets = getTargets(sender);
        List<String> responses = new ArrayList<>();
        String res;
        for(IPv4Address a: targets){
            res = sendNodeRequest(a, "get-min "+value,id);
            if(!res.equals(MESSAGE_ERROR)) responses.add(res);
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
            sendNodeRequest(a, "terminate",id);
        }
        return MESSAGE_OK;
    }

    // Sends a request to all connections and returns the responses
    private String sendNodeRequest(IPv4Address address ,String request, int id){
        printMessage(String.valueOf(serverAddress.getPort()), "Sending \""+request+"\" request to: "+address.getPort());
        String res;
        Socket socket;
        try{
            socket = new Socket(address.getIp(), address.getPort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println("node//"+request+"//"+id+"//"+serverAddress);
            // Read and print out the response
            res = in.readLine();
            return res;
        } catch (IOException e) {
            return MESSAGE_ERROR;
        }
    }

    private String getMessageFromResponse(String res){
        if(res.contains("node//")){
            String[] parts = res.split("//");
            return parts[1];
        }
        else return res;
    }

    public List<IPv4Address> getTargets(IPv4Address sender){
        List<IPv4Address> list = new ArrayList<>(connections);
        if(sender != null){
            int index = -1;
            for(int i = 0;i < list.size() ;i++){
                if(list.get(i).equals(sender)){
                    index = i;
                    break;
                }
            }
            if(index >= 0) list.remove(index);
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

