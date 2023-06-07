package it.unitn.disi.marchioro.mousavi;

import it.unitn.disi.marchioro.mousavi.Node.JoinNodeCoordinator;
import it.unitn.disi.marchioro.mousavi.Node.LeaveNodeCoordinator;
// import it.unitn.disi.marchioro.mousavi.Node.*;
// import it.unitn.disi.marchioro.mousavi.SortedCircularDoublyLinkedList.*;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.util.ArrayList;

public class Solution {
    public final static int M = 5, N = 3, R = 2, W = 2, T = 10;

    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("akkasystem");

        SortedCircularDoublyLinkedList<ActorRef> actorList = new SortedCircularDoublyLinkedList<ActorRef>();

        // create M nodes
        int id, key;
        for (int i = 1; i <= M; i++) {
            try {
                Thread.sleep(1000); // wait for 1 second
            } catch (InterruptedException e) {
                // handle the exception
            }
            
            id = i;
            key = i * 10;
            ActorRef actorRef = system.actorOf(Node.props(id, key), "node" + id);
            actorList.add(key, actorRef);
            
            // If this is the first node, there is no other node to tell it to join
            if (i == 1) {
                continue;
            }

            JoinNodeCoordinator joinNodeCoordinator = new JoinNodeCoordinator(key, actorRef);
            ActorRef headRef = actorList.getFirst().value;
            headRef.tell(joinNodeCoordinator, ActorRef.noSender());
        }

        try {
            Thread.sleep(1000); // wait for 1 second
        } catch (InterruptedException e) {
            // handle the exception
        }

        LeaveNodeCoordinator leaveNodeCoordinator = new LeaveNodeCoordinator(40);
        ActorRef headRef = actorList.getFirst().value;
        headRef.tell(leaveNodeCoordinator, ActorRef.noSender());
        



    }

}
