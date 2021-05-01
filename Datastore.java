import java.util.ArrayList;

public class Datastore {
    private int port;
    private boolean index;
    private ArrayList<String> fileNames;

    public Datastore(int port, boolean index, ArrayList<String> fileNames) {
        this.port = port;
        this.index = index;
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

    public ArrayList<String> getFileNames() {
        return fileNames;
    }

    public void setFileNames(ArrayList<String> fileNames) {
        this.fileNames = fileNames;
    }

    public void addFileName(String fileName) {
        this.fileNames.add(fileName);
    }
}
