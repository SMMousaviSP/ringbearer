package it.unitn.disi.marchioro.mousavi;

public class DataItem implements Cloneable{
    private int key;
    private String value;
    private int version;
    private boolean lock;

    public DataItem(int key, String value, int version) {
        this(key,value,version,false);
    }

    public DataItem(int key, String value, int version, boolean lock) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.lock=lock;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return "DataItem{" +
                "key=" + key +
                ", value='" + value + '\'' +
                ", version=" + version +
                '}';
    }

    public boolean isLock() {
        return lock;
    }

    public void setLock(boolean lock) {
        this.lock = lock;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
