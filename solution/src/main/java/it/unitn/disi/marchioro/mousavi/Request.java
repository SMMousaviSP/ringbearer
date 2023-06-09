package it.unitn.disi.marchioro.mousavi;

import scala.collection.immutable.Stream;

enum State{STARTING,PENDING,COMMITTING,ENDING}
enum Type{READ,UPDATE}

public class Request {
    private int key;
    private String value;
    private State state;
    private int locks;
    private Type type;
    private int response_count;

    public Request(int key,String value, Type type) {
        this.key=key;
        this.value=value;
        this.state=State.STARTING;
        this.locks=0;
        this.type = type;
        this.response_count=0;
    }
    public Request(int key, Type type) {
        this(key,"",type);
    }

    public int getKey() {
        return key;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public int getLocks() {
        return locks;
    }

    public Type getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    public int receivedResponse(){
        response_count++;
        return response_count;
    }
    public int acquiredLock(){
        locks++;
        return locks;
    }
    public boolean mayBePerformed(){
        int minimum=type==Type.READ? Constants.R:Constants.W;
        return locks+Constants.N-response_count>=minimum;
    }
    public boolean canCommit(){
        int minimum=type==Type.READ? Constants.R:Constants.W;
        return locks>=minimum; //maybe should it should be == to avoid multiple commits of the same dataitem
    }
}
