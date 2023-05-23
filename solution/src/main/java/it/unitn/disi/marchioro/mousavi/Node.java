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
    private List<ActorRef> group;   // an array of group members

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
                .build();
    }
    private void onGroupUpdateMessage(GroupUpdateMessage msg) {
        // initialize group
        this.group= msg.group;
        System.out.println(this.id+" received a new system configuration. Group size: "+group.size());
    }
    //Message used to communicate group updates: Nodes joining/leaving, also used to define initial system configuration
    public static class GroupUpdateMessage implements Serializable{
        public final List<ActorRef> group;   // an array of group members
        public GroupUpdateMessage(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }


}
