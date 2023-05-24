package it.unitn.disi.marchioro.mousavi;
import it.unitn.disi.marchioro.mousavi.Node.*;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.util.ArrayList;
import java.util.List;

public class Solution {
    public final static int M=5, N=3,R=2,W=2,T=10;
    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("akkasystem");

        CircularLinkedList actorList= new CircularLinkedList();

        int id = 0;

        //create M nodes
        for (int i = 0; i < M; i++) {
            actorList.addNode(100-(id*10),system.actorOf(Node.props(id++, 100-(id*10)),"node"+i));
        }
        actorList.printList();
        //Communicate initial configuration to every Node
        GroupUpdateMessage initialGroup= new GroupUpdateMessage(actorList);
        CircularLinkedList.ListNode temp = actorList.head;
        do
        {
            temp.getActorRef().tell(initialGroup,ActorRef.noSender());
            //System.out.println("");
            temp = temp.next;
        }  while (temp != actorList.head);
        actorList.head.getActorRef().tell(new DataUpdateMessage(1,"ciao"),ActorRef.noSender());
        actorList.head.getActorRef().tell(new DataUpdateMessage(2,"ciao2"),ActorRef.noSender());
        actorList.head.getActorRef().tell(new DataUpdateMessage(1,"ciao1"),ActorRef.noSender());
        actorList.head.next.getActorRef().tell(new DataUpdateMessage(1,"ciao3"),ActorRef.noSender());

        // system shutdown
        system.terminate();
    }
}
