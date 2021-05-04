import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;

public class Datastore {
    private int port;
    private boolean index;
    private Socket socket;
    private ArrayList<String> fileNames;

    public Datastore(int port, boolean index, Socket socket, ArrayList<String> fileNames) {
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

    public ArrayList<String> getFileNames() {
        return fileNames;
    }

    public void setFileNames(ArrayList<String> fileNames) {
        this.fileNames = fileNames;
    }

    public void setFileNames(String[] fileNames) {
        this.fileNames = new ArrayList<>();
        Collections.addAll(this.fileNames, fileNames);
    }

    public void addFileName(String fileName) {
        this.fileNames.add(fileName);
    }

    public void removeFileName(String fileName) {
        this.fileNames.remove(fileName);
    }
}
