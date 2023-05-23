package it.unitn.disi.marchioro.mousavi;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.ArrayList;
import java.util.List;

public class Solution {
    final static private int M=5, N=3,R=2,W=2,T=10;
    public static void main(String[] args) {
        // Create the 'helloakka' actor system
        final ActorSystem system = ActorSystem.create("helloakka");

        List<ActorRef> group = new ArrayList<>();
        int id = 0;

        //create M nodes
        for (int i = 0; i < M; i++) {
            group.add(system.actorOf(Node.props(id++, id*10),"node"+i));
        }

        //Communicate initial configuration to every Node
        Node.GroupUpdateMessage initialGroup= new Node.GroupUpdateMessage(group);
        for(ActorRef a :group){
            a.tell(initialGroup,ActorRef.noSender());
        }

    }
}
