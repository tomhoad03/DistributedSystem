import java.util.ArrayList;

public class DstoreClass {
    private int port;
    private boolean index;
    private ArrayList<String> fileNames;

    public DstoreClass(int port, boolean index, ArrayList<String> fileNames) {
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

    public boolean isIndex() {
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
}
