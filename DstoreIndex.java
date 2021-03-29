import java.util.ArrayList;

public class DstoreIndex {
    private int port;
    private String index;
    private ArrayList<String> fileNames;

    public DstoreIndex(String port, String index) {
        this.port = Integer.parseInt(port);
        this.index = index;
        this.fileNames = new ArrayList<>();
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

    public void setPort(int port) {
        this.port = port;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setFileNames(ArrayList<String> fileNames) {
        this.fileNames = fileNames;
    }

    public void addFileName(String fileName) {
        this.fileNames.add(fileName);
    }
}
