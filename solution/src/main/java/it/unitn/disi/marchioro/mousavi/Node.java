package it.unitn.disi.marchioro.mousavi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Console;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Node extends AbstractActor {
    private int id, key;
    private HashMap<Integer,DataItem> storage;
    private CircularLinkedList group;   // an array of group members

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    //constructors
    public Node(int id, int key) {
        this(id, key,new HashMap<Integer,String>());
    }
    public Node(int id, int key, HashMap<Integer,String> storage) {
        this.id = id;
        this.key = key;
        this.storage = (HashMap<Integer, DataItem>) storage.clone();
    }

    static public Props props(int id, int value) {
        return Props.create(Node.class, () -> new Node(id, value));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GroupUpdateMessage.class, this::onGroupUpdateMessage)
                .match(DataUpdateMessage.class, this::onDataUpdateMessage)
                .build();
    }
    public Receive crashed(){
        return receiveBuilder()
        .matchAny(msg -> {})// if crashed ignore all messages
                //.matchAny(msg -> System.out.println(getSelf().path().name() + " ignoring " + msg.getClass().getSimpleName() + " (crashed)"))
                .build();
    }
    private void onGroupUpdateMessage(GroupUpdateMessage msg) {
        // initialize group
        this.group= msg.group;
        group.printList();
    }
    private void propagateUpdate(DataUpdateMessage msg){
        int c=0;
    }
    private void onDataUpdateMessage(DataUpdateMessage msg) {
        //TODO: modify method
        if(getSender().path().name().startsWith("node")){//this means the event has been triggered by another Server

        }else {//this means the event has been triggered by a request from a Client

            if (storage.containsKey(msg.key)) {
                DataItem storedItem = storage.get(msg.key);
                storedItem.setVersion(storedItem.getVersion() + 1);
                storedItem.setValue(msg.value);
            } else {
                storage.put(msg.key, new DataItem(msg.key, msg.value, 0));
            }
            for (DataItem d : storage.values()) {
                System.out.println(this.id + ": " + d.toString());
            }
        }
    }
    //Message used to communicate group updates: Nodes joining/leaving, also used to define initial system configuration
    public static class GroupUpdateMessage implements Serializable{
        public final CircularLinkedList group;   // an array of group members
        public GroupUpdateMessage(CircularLinkedList group) {
            this.group =group;
        }
    }
    public static class DataUpdateMessage implements Serializable{
        public int key;
        public String value;
        public DataUpdateMessage(int key, String value) {
            this.key = key;
            this.value=value;
        }
    }


}
