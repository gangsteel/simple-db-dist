package distributeddb;

import networking.HeadNode;
import simpledb.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by aditisri on 12/11/17.
 */
public class Profiler {
    public enum Type{
        SOCKET,
        PROCESSING,
        TRANSFER,
    }
    private Map<Type, Long> timeAllocationMap;
    public Profiler(){
        timeAllocationMap = new ConcurrentHashMap<>();
        for(Type type: Type.values()){
            timeAllocationMap.put(type, 0l);
        }
    }

    public void incrementType(Type t, long time){
        System.out.println(t + " " + time);
        if (time < 0){
            System.out.println("bad time");
            return;
        }
        synchronized (this.timeAllocationMap){
            timeAllocationMap.put(t, timeAllocationMap.get(t) + time);
        }
    }

    public void printStats(){
        for (Type category: this.timeAllocationMap.keySet()){
            System.out.println(category + ": " + this.timeAllocationMap.get(category)+ "ns");
        }
    }



}
