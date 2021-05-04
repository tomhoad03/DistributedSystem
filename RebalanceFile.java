import java.util.HashSet;

public class RebalanceFile {
    private String fileName;
    private int sourcePort;
    private HashSet<Integer> destinationPorts = new HashSet<>();

    public RebalanceFile(String fileName, int sourcePort) {
        this.fileName = fileName;
        this.sourcePort = sourcePort;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }

    public HashSet<Integer> getDestinationPorts() {
        return destinationPorts;
    }

    public void setDestinationPorts(HashSet<Integer> destinationPorts) {
        this.destinationPorts = destinationPorts;
    }

    public void addDestinationPort(int destinationPort) {
        this.destinationPorts.add(destinationPort);
    }
}
