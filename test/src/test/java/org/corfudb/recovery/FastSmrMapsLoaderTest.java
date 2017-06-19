package org.corfudb.recovery;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectsView;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by rmichoud on 6/16/17.
 */
public class FastSmrMapsLoaderTest extends AbstractViewTest {


    private VersionLockedObject getVersionLockedObject(CorfuRuntime cr, String streamName) {
        ObjectsView.ObjectID mapId = new ObjectsView.
                ObjectID(CorfuRuntime.getStreamID(streamName), SMRMap.class);

        CorfuCompileProxy cp = ((CorfuCompileProxy) ((ICorfuSMR) cr.getObjectsView().
                getObjectCache().
                get(mapId)).
                getCorfuSMRProxy());
        return cp.getUnderlyingObject();
    }

    private Map<String, String> createMap(String streamName, CorfuRuntime cr) {
        return cr.getObjectsView().build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();
    }

    private void assertThatMapIsBuilt(CorfuRuntime rt1, CorfuRuntime rt2, String streamName,
                                      Map<String, String> map) {

        // Get raw maps (VersionLockedObject)
        VersionLockedObject vo1 = getVersionLockedObject(rt1, streamName);
        VersionLockedObject vo1Prime = getVersionLockedObject(rt2, streamName);

        // Assert that UnderlyingObjects are at the same version
        // If they are at the same version, a sync on the object will
        // be a no op for the new runtime.
        assertThat(vo1.getVersionUnsafe()).isEqualTo(vo1Prime.getVersionUnsafe());
        assertThat(rt1.getObjectsView().getObjectCache().size())
                .isEqualTo(rt2.getObjectsView().getObjectCache().size());

        Map<String, String> mapPrime = createMap(streamName, rt2);
        assertThat(map.size()).isEqualTo(mapPrime.size());
        map.forEach((key, value) -> assertThat(value).isEqualTo(mapPrime.get(key)));
    }


    /** Test that the maps are reloaded after runtime.connect()
     *
     * By checking the version of the underlying objects, we ensure that
     * they are the same in the previous runtime and the new one.
     *
     * If they are at the same version, upon access the map will not be
     * modified. All the subsequent asserts are on the pre-built map.
     *
     * @throws Exception
     */
    @Test
    public void canReloadMaps() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        Map<String, String> map2 = createMap("Map2", rt1);

        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map2.put("k3", "v3");
        map2.put("k4", "v4");

        // Create a new runtime with fastloader
        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);
        assertThatMapIsBuilt(rt1, rt2, "Map2", map2);

    }

    @Test
    public void canReadWithCacheDisable() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .setCacheDisabled(true)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);


    }



    /**
     * This test ensure that reading Holes will not affect the recovery.
     *
     * @throws Exception
     */
    @Test
    public void canReadHoles() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");

        LogUnitClient luc = rt1.getRouter(getDefaultConfigurationString())
                .getClient(LogUnitClient.class);
        SequencerClient seq = rt1.getRouter(getDefaultConfigurationString())
                .getClient(SequencerClient.class);

        seq.nextToken(null, 1);
        luc.fillHole(rt1.getSequencerView()
                .nextToken(Collections.emptySet(), 0)
                .getTokenValue());

        map1.put("k3", "v3");

        seq.nextToken(null, 1);
        luc.fillHole(rt1.getSequencerView()
                .nextToken(Collections.emptySet(), 0)
                .getTokenValue());

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .connect();

     assertThatMapIsBuilt(rt1, rt2, "Map1", map1);

    }

    /** Ensures that we are able to do rollback after the fast loading
     *
     * It is by design that reading the entries will not populate the
     * streamView context queue. Snapshot transaction to addresses before
     * the initialization log tail should seldom happen.
     *
     * In the case it does happen, the stream view will follow the backpointers
     * to figure out which are the addresses that it should rollback to.
     *
     * We don't need to optimize this path, since we don't need the stream view context
     * queue to be in a steady state. If the user really want snapshot back in time,
     * he/she will take the hit.
     *
     * @throws Exception
     */
    @Test
    public void canRollBack() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .getParameters()
                .setLoadSmrMapsAtConnect(true)
                .connect();

        Map<String, String> map1Prime = createMap("Map1", rt2);

        rt2.getObjectsView().TXBuild().setType(TransactionType.SNAPSHOT).setSnapshot(0).begin();
        assertThat(map1Prime.get("k1")).isEqualTo("v1");
        assertThat(map1Prime.get("k2")).isNull();
        rt2.getObjectsView().TXEnd();

    }

    @Test
    public void canLoadWithCustomBulkRead() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();
        Map<String, String> map1 = createMap("Map1", rt1);
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map1.put("k3", "v3");
        map1.put("k4", "v4");

        CorfuRuntime rt2 = new CorfuRuntime(getDefaultConfigurationString())
                .setLoadSmrMapsAtConnect(true)
                .setBulkReadSizeForFastLoader(2l)
                .connect();

        assertThatMapIsBuilt(rt1, rt2, "Map1", map1);




    }

}
