package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.*;
import org.corfudb.runtime.Payload;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by rmichoud on 6/17/17.
 */
public class FastLoadIt {

    String configStr = "localhost:9000";
    @Getter
    private CorfuRuntime runtime = new CorfuRuntime(configStr).setCacheDisabled(true).connect();


    private Map<String, org.corfudb.runtime.Payload> createMap(String streamName, CorfuRuntime cr) {
        return cr.getObjectsView().build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<SMRMap<String, Payload>>() {
                })
                .open();
    }

    private CorfuRuntime getDefaultRuntime() {
        String configStr = "localhost:9000";
        return new CorfuRuntime(configStr).setCacheDisabled(true).connect();
    }



    @Test
    public void createCheckPointedLog() throws Exception {
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("test")
                .open();

        // Place 3 entries into the map
        testMap.put("a", "a");
        testMap.put("b", "b");
        testMap.put("c", "c");

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((SMRMap) testMap);
        long checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress - 1);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

    }

    @Test
    public void putSomeDataOnDisk() throws Exception {
        String configStr = "localhost:9000";
        CorfuRuntime crt = new CorfuRuntime(configStr).setCacheDisabled(true).connect();

        List<Map<String, Payload>> maps = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            maps.add(createMap("testM" + Integer.toString(i), crt));
        }

        int mapPosition = -1;
        Map<String, Payload> testM;

        for (int i = 0; i < 1000; i++) {
            System.out.println("Round: " + i);
            mapPosition = (mapPosition + 1) % 400;
            testM = maps.get(mapPosition);

            crt.getObjectsView().TXBegin();
            testM.put(Integer.toString(i), new org.corfudb.runtime.Payload());
            testM.put(Integer.toString(i) + "_bis", new org.corfudb.runtime.Payload());
            crt.getObjectsView().TXEnd();

            // No more than 10 entries in the map (for memory)
            if (testM.size() >= 10) {
                testM.clear();
            }
        }
    }

    @Test
    public void loadWithUtility() throws Exception {
        org.corfudb.runtime.Payload p = new org.corfudb.runtime.Payload();
        long t = System.currentTimeMillis();
        CorfuRuntime runtime = new CorfuRuntime("localhost:9000")
                .setLoadSmrMapsAtConnect(true)
//                .setCacheDisabled(true)
                .connect();

//        Map<String, org.corfudb.runtime.Payload> testM =
//                    createMap("testM" + Integer.toString(0), runtime);
//        System.out.println(testM.size());
        for (int i = 0; i < 400; i++) {

            Map<String, org.corfudb.runtime.Payload> testM =
                    createMap("testM" + Integer.toString(i), runtime);
//            testM.put("1", new Payload());
//            testM.put("2", new Payload());
            System.out.println("Map " + "testM" + Integer.toString(i));
//            System.out.println("size: " + testM.size());
            testM.size();
        }

        System.out.println("time to load: " + (System.currentTimeMillis() - t));

//        t = System.currentTimeMillis();
//        for (int i = 0; i < 400; i++) {
//
//            Map<String, org.corfudb.runtime.Payload> testM =
//                    createMap("testM" + Integer.toString(i), runtime);
////            testM.put("1", new Payload());
////            testM.put("2", new Payload());
//            System.out.println("Map " + "testM" + Integer.toString(i));
//            System.out.println("size: " + testM.size());
//        }

//        Map<String, org.corfudb.runtime.Payload> testM = createMap("testM", runtime);
//        t = System.currentTimeMillis();
//        System.out.println(testM.size());
//        System.out.println("time to read: " + (System.currentTimeMillis() - t));
    }


    @Test
    public void LoadWithoutUtility() throws Exception {
        org.corfudb.runtime.Payload p = new org.corfudb.runtime.Payload();
        long t = System.currentTimeMillis();
        CorfuRuntime runtime = new CorfuRuntime("localhost:9000")
//                setLoadSmrMapsAtConnect(true)
//                .setCacheDisabled(true)
                .connect();

        for (int i = 0; i < 400; i++) {

            Map<String, org.corfudb.runtime.Payload> testM =
                    createMap("testM" + Integer.toString(i), runtime);
//            testM.put("1", new Payload());
//            testM.put("2", new Payload());
            System.out.println("Map " + "testM" + Integer.toString(i));
//            System.out.println("size: " + testM.size());
            testM.size();
        }

        System.out.println("time to load: " + (System.currentTimeMillis() - t));
    }
    @Test
    public void LoadWithUtilityDummy() throws Exception {
        Serializers.registerSerializer(new DummySerializer());
        CorfuRuntime rt = new CorfuRuntime("localhost:9000")
//                .setLoadSmrMapsAtConnect(true)
                .setCacheDisabled(true)
                .connect();
    }
}
