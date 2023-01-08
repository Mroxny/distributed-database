public class IPv4Address {
    private String ip;
    private int port;

    public IPv4Address(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }
    public IPv4Address(String combined){
        this(combined.split(":")[0], Integer.parseInt(combined.split(":")[1]));
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean equals(IPv4Address address){
        return ip.equals(address.ip) && port == address.port;
    }
    @Override
    public String toString() {
        return ip+":"+port;
    }
}
