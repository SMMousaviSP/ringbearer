package it.unitn.disi.marchioro.mousavi;

enum State{STARTING,PENDING,COMMITTING,ENDING}
enum Type{READ,UPDATE}

public class Request {
    private int key;
    private String value;
    private State state;
    private int locks;
    private Type type;

    public Request(int key,String value, Type type) {
        this.key=key;
        this.value=value;
        this.state=State.STARTING;
        this.locks=0;
        this.type = type;
    }

    public int getKey() {
        return key;
    }

    public State getState() {
        return state;
    }

    public int getLocks() {
        return locks;
    }

    public Type getType() {
        return type;
    }
}
