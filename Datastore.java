import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class Datastore {
    private int port;
    private String index;
    private ArrayList<String> fileNames;
    private int numFiles;
    private Map<String, ArrayList<String>> toSend;
    private ArrayList<String> toRemove;

    public Datastore(String port, String index) {
        this.port = Integer.parseInt(port);
        this.index = index;
        this.fileNames = new ArrayList<>();
        this.numFiles = 0;
    }

    public int getPort() {
        return port;
    }

    public String getIndex() {
        return index;
    }

    public ArrayList<String> getFileNames() {
        return fileNames;
    }

    public int getNumFiles() {
        return numFiles;
    }

    public Map<String, ArrayList<String>> getToSend() {
        return toSend;
    }

    public ArrayList<String> getToRemove() {
        return toRemove;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setFileNames(ArrayList<String> fileNames) {
        this.fileNames = fileNames;
        this.numFiles = fileNames.size();
    }

    public void addFileName(String fileName) {
        this.fileNames.add(fileName);
        this.numFiles++;
    }

    public void removeFileName(String fileName) {
        this.fileNames.remove(fileName);
        this.numFiles--;
    }

    public void addToSend(String file, String port) {
        if (toSend.containsKey(file)) {
            this.toSend.get(file).add(port);
        } else {
            this.toSend.put(file, new ArrayList<>(Collections.singleton(port)));
        }
    }

    public void removeToSend(String toSend) {
        this.toSend.remove(toSend);
    }

    public void addToRemove(String toRemove) {
        this.toRemove.add(toRemove);
    }

    public void removeToRemove(String toRemove) {
        this.toRemove.remove(toRemove);
    }
}
