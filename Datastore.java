import java.util.ArrayList;

public class Datastore {
    private int port;
    private String index;
    private Controller.ControllerThread thread;
    private ArrayList<String> fileNames;

    public Datastore(String port, String index, Controller.ControllerThread thread) {
        this.port = Integer.parseInt(port);
        this.index = index;
        this.thread = thread;
        this.fileNames = new ArrayList<>();
    }

    public int getPort() {
        return port;
    }

    public String getIndex() {
        return index;
    }

    public Controller.ControllerThread getThread() {
        return thread;
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

    public void setThread(Controller.ControllerThread thread) {
        this.thread = thread;
    }

    public void setFileNames(ArrayList<String> fileNames) {
        this.fileNames = fileNames;
    }

    public void addFileName(String fileName) {
        this.fileNames.add(fileName);
    }

    public void removeFileName(String fileName) {
        this.fileNames.remove(fileName);
    }
}
