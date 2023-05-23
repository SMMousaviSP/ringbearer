package it.unitn.disi.marchioro.mousavi;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Node extends AbstractActor {
    private int id,value;
    @Override
    public Receive createReceive() {
        return null;
    }
    public Node(int id, int value) {
        this.id = id;
        this.value = value;
    }
    static public Props props(int id, int value) {
        return Props.create(Node.class, () -> new Node(id, value));
    }
}
