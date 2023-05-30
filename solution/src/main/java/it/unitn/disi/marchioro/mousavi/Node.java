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
                .match(VoteResponse.class,this::onVoteResponse)
                .match(Commitmsg.class,this::onCommitmsg)
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
    private int locked=0;
    private int handlingkey=-1;
    private HashMap<Integer, CircularLinkedList.ListNode> handlers=null;
    private void onVoteResponse(VoteResponse voteResponse){
        if(handlingkey!=-1 && voteResponse.success){
            locked++;
        }else{
            locked=0;
        }
        if(locked>=Solution.W){
            System.out.println(getId()+" received locks from "+ locked+ " nodes, proceeding with commit");
            for(CircularLinkedList.ListNode ln : handlers.values()){
                ln.getActorRef().tell(new Commitmsg(voteResponse.msg),getSelf());
            }
            locked=0;
        }
    }
    private void onCommitmsg(Commitmsg commitmsg){
        if(storage.containsKey(commitmsg.msg.key)) {
            storage.get(commitmsg.msg.key).setValue(commitmsg.msg.value);
        }
        else{
            storage.put(commitmsg.msg.key,new DataItem(commitmsg.msg.key,commitmsg.msg.value, 0));
        }
        System.out.println(getId()+" committed "+commitmsg.msg.value+ " on data "+commitmsg.msg.key);
    }
    private void onDataUpdateMessage(DataUpdateMessage msg) {
        //TODO: modify method
        if(getSender().path().name().startsWith("node")){//this means the event has been triggered by another Server
            //if request arrives from another server send back to it the information regarding the data item
            if(storage.containsKey(msg.key)){
                if(!storage.get(msg.key).isLocked()){
                    storage.get(msg.key).Lock(1);//change it with UUID;
                    getSender().tell(new VoteResponse(true,msg),getSelf());
                }else{
                    getSender().tell(new VoteResponse(false,msg),getSelf());
                }
            }
            else{
                getSender().tell(new VoteResponse(true,msg),getSelf());
            }
        }else {//this means the event has been triggered by a request from a Client
            //if received from a client, ask other servers, collect data and then send back to client (check for timeout)
            handlers=group.getHandlers(msg.key,Solution.N);
            handlingkey=msg.key;
            for(CircularLinkedList.ListNode ln : handlers.values()){
                ln.getActorRef().tell(msg,getSelf());
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
    public static class VoteResponse implements Serializable {
        public final boolean success;
        public final DataUpdateMessage msg;
        public VoteResponse(boolean success,DataUpdateMessage msg) { this.success = success; this.msg=msg;}
    }

    public static class Commitmsg implements Serializable {
        public final DataUpdateMessage msg;
        public Commitmsg(DataUpdateMessage msg) {  this.msg=msg;}
    }


}
