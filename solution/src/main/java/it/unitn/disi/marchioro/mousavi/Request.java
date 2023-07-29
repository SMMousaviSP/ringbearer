package it.unitn.disi.marchioro.mousavi;

import akka.actor.ActorRef;
import scala.collection.immutable.Stream;

import javax.xml.crypto.Data;

enum State {
    STARTING, PENDING, COMMITTING, ENDING
}

enum Type {
    READ, UPDATE
}

public class Request {
    private State state;
    private int locks;
    private Type type;
    private int response_count;
    private DataItem data;
    private ActorRef client;

    public Request(int key, String value, Type type,ActorRef client) {
        this.data=new DataItem(key,value,-1);
        this.state = State.STARTING;
        this.locks = 0;
        this.type = type;
        this.response_count = 0;
        this.client=client;
    }

    public Request(int key, Type type,ActorRef client) {
        this(key, "", type,client);
    }


    public ActorRef getClient() {
        return client;
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

    public DataItem getData() {
        return data;
    }

    public void setData(DataItem data) {
        this.data = data;
    }

    public int receivedResponse() {
        response_count++;
        return response_count;
    }

    public int acquiredLock() {
        locks++;
        return locks;
    }

    public boolean mayBePerformed() {
        int minimum = type == Type.READ ? Constants.R : Constants.W;
        return locks + Constants.N - response_count >= minimum;
    }

    public boolean canCommit() {
        int minimum = type == Type.READ ? Constants.R : Constants.W;
        return locks >= minimum; // maybe should it should be == to avoid multiple commits of the same dataitem
    }

    @Override
    public String toString() {
        return "Request{" +
                "state=" + state +
                ", locks=" + locks +
                ", type=" + type +
                ", response_count=" + response_count +
                ", data=" + data +
                ", client=" + client +
                '}';
    }
}
