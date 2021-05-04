public class DatastoreFile {
    private String fileName;
    private int fileSize;
    private boolean found;

    public DatastoreFile(String fileName, int fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.found = false;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public boolean isFound() {
        return found;
    }

    public void setFound(boolean found) {
        this.found = found;
    }
}
