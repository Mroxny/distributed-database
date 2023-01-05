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

    public DatabaseNode(int tcpPort, int key, int value, List<IPv4Address> addresses) throws IOException {
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
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    handleClient(clientSocket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
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
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream());

        String line = in.readLine();
        String[] parts = line.split(" ");
        String operation = parts[0];

        if (operation.equals("set-value")) {
            int key = Integer.parseInt(parts[1]);
            int value = Integer.parseInt(parts[2]);
            data.put(key, value);
            out.println("OK");
        } else if (operation.equals("get-value")) {
            int key = Integer.parseInt(parts[1]);
            if (data.containsKey(key)) {
                out.println(data.get(key));
            } else {
                out.println("ERROR");
            }
        } else {
            // Handle multi-key request
            List<String> responses = handleMultiKeyRequest(line);
            for (String response : responses) {
                out.println(response);
            }
        }

        out.flush();
        socket.close();
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

    // Handles a multi-key request by forwarding it to all connections and returning the responses
    private List<String> handleMultiKeyRequest(String line) throws IOException {
        String[] parts = line.split(" ");
        String operation = parts[0];
        List<String> responses = new ArrayList<>();

        if (operation.equals("get-values")) {
            // Forward request to all connections
            List<String> tempResponses = sendRequest(line);

            // Process responses and add values to list
            for (String tempResponse : tempResponses) {
                if (!tempResponse.equals("ERROR")) {
                    String[] tempParts = tempResponse.split(" ");
                    responses.addAll(Arrays.asList(tempParts).subList(1, tempParts.length));
                }
            }
        } else if (operation.equals("set-values")) {
            // Set values in own data
            for (int i = 1; i < parts.length; i++) {
                String[] keyValue = parts[i].split(":");
                int key = Integer.parseInt(keyValue[0]);
                int value = Integer.parseInt(keyValue[1]);
                data.put(key, value);
            }

            // Forward request to all connections
            responses = sendRequest(line);
        }

        return responses;
    }

}

