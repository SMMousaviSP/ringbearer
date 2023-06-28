package it.unitn.disi.marchioro.mousavi;

import akka.actor.AbstractActor;
import akka.actor.Props;

import javax.xml.crypto.Data;


public class Client extends AbstractActor {
    private int id;
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DataItem.class, this::onDataItem)
                .build();
    }

    private void onDataItem(DataItem dataItem) {
        if(dataItem.getVersion()!=-1){
            System.out.println("client received dataitem "+dataItem.toString());
        }
    }

    public Client(int id) {
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }
}
