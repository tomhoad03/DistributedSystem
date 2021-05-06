import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;

public class Datastore {
    private int port;
    private boolean index;
    private Socket socket;
    private HashSet<String> fileNames;

    private int rebalanceCount = 0;
    private HashSet<RebalanceFile> sendFiles = new HashSet<>();
    private HashSet<String> keepFiles = new HashSet<>();
    private HashSet<String> removeFiles = new HashSet<>();

    public Datastore(int port, boolean index, Socket socket, HashSet<String> fileNames) {
        this.port = port;
        this.index = index;
        this.socket = socket;
        this.fileNames = fileNames;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean getIndex() {
        return index;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public HashSet<String> getFileNames() {
        return fileNames;
    }

    public void setFileNames(HashSet<String> fileNames) {
        this.fileNames = fileNames;
    }

    public void setFileNames(String[] fileNames) {
        this.fileNames = new HashSet<>();
        if (!fileNames[0].equals("")) {
            Collections.addAll(this.fileNames, fileNames);
        }
    }

    public void addFileName(String fileName) {
        this.fileNames.add(fileName);
    }

    public void removeFileName(String fileName) {
        this.fileNames.remove(fileName);
    }

    public void newRebalance() {
        rebalanceCount = 0;
        sendFiles = new HashSet<>();
        keepFiles = new HashSet<>();
        removeFiles = new HashSet<>();
        removeFiles.addAll(fileNames);
    }

    public void newSend(String fileName) {
        sendFiles.add(new RebalanceFile(fileName, port));
        rebalanceCount++;
    }

    public HashSet<RebalanceFile> getSendFiles() {
        return sendFiles;
    }

    public HashSet<String> getKeepFiles() {
        return keepFiles;
    }

    public HashSet<String> getRemoveFiles() {
        return removeFiles;
    }

    public void removeRemoveFile(String removeFile) {
        removeFiles.remove(removeFile);
    }

    public boolean containsSendFile(String fileName) {
        for (RebalanceFile sendFile : sendFiles) {
            if (sendFile.getFileName().equals(fileName)) {
                return true;
            }
        }
        return false;
    }

    public void newKeep(String fileName) {
        keepFiles.add(fileName);
        rebalanceCount++;
    }

    public void newReceive() {
        rebalanceCount++;
    }

    public int getRebalanceCount() {
        return rebalanceCount;
    }

    public boolean alreadyStores(String fileName) {
        return fileNames.contains(fileName);
    }

    public String finishRebalance() {
        // identifies files to remove
        HashSet<String> toRemove = new HashSet<>();
        for (String removeFile : removeFiles) {
            boolean remove = true;
            for (RebalanceFile sendFile : sendFiles) {
                if (sendFile.getFileName().equals(removeFile)) {
                    remove = false;
                    break;
                }
            }
            if (!remove || keepFiles.contains(removeFile)) {
                toRemove.add(removeFile);
            }
        }
        for (String removeFile : toRemove) {
            removeFiles.remove(removeFile);
        }

        // creates rebalance string
        StringBuilder portsMsg = new StringBuilder();
        int sendCount = sendFiles.size();

        for (RebalanceFile sendFile : sendFiles) {
            if (sendFile.getDestinationPorts().size() > 0) {
                portsMsg.append(" ").append(sendFile.getFileName()).append(" ").append(sendFile.getDestinationPorts().size());
            } else {
                sendCount--;
            }
            for (Integer destinationPort : sendFile.getDestinationPorts()) {
                portsMsg.append(" ").append(destinationPort);
            }
        }
        portsMsg.append(" ").append(removeFiles.size());
        for (String removeFile : removeFiles) {
            portsMsg.append(" ").append(removeFile);
        }
        return "REBALANCE" + " " + sendCount + portsMsg;
    }
}
