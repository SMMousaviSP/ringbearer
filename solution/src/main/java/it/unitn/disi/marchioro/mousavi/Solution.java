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
            actorList.addNode(id*10,system.actorOf(Node.props(id++, id*10),"node"+i));
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
        try{
            actorList.get(0).getActorRef().tell(new DataUpdateMessage(1,"ciao"),ActorRef.noSender());
            actorList.get(0).getActorRef().tell(new DataUpdateMessage(2,"ciao2"),ActorRef.noSender());
            actorList.get(0).getActorRef().tell(new DataUpdateMessage(1,"ciao1"),ActorRef.noSender());
            actorList.get(1).getActorRef().tell(new DataUpdateMessage(1,"ciao3"),ActorRef.noSender());
            actorList.get(4).getActorRef().tell(new DataUpdateMessage(1,"ciao4"),ActorRef.noSender());
           // actorList.get(5).getActorRef().tell(new DataUpdateMessage(1,"ciao5"),ActorRef.noSender()); // this should raise an exception when there are only 5 actors
        }catch (Exception e){
            e.printStackTrace();
        }


        // system shutdown
        system.terminate();
    }
}
