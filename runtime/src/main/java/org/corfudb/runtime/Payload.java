package org.corfudb.runtime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by rmichoud on 6/17/17.
 */

public class Payload {
    private long f1 = 1;
    private long f2 = 2;
    private long f4 = 3;
    private long f5 = 4;
    private long f6 = 5;

    private UUID f7 = new UUID(0, 0);
    private UUID f8 = new UUID(0, 0);
    private UUID f9 = new UUID(0, 0);
    private UUID f10 = new UUID(0, 0);
    private UUID f11 = new UUID(0, 0);

    private String f12 = "This is a string, This is a string, This is a string, This is a string, This is a string";
    private String f13 = "This is a string, This is a string, This is a string, This is a string, This is a string";
    private String f14 = "This is a string, This is a string, This is a string, This is a string, This is a string";
    private String f15 = "This is a string, This is a string, This is a string, This is a string, This is a string";
    private String f16 = "This is a string, This is a string, This is a string, This is a string, This is a string";

    private Set<String> set;
    private HashMap<String, String> map;

    public Payload() {
        set = new HashSet();
        map = new HashMap();

        for(int x = 0; x < 140; x++) {
            set.add("This is a string" + x);
            map.put("key" + x, "string value");
        }
    }

}


