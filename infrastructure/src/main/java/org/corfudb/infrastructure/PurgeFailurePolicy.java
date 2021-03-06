package org.corfudb.infrastructure;

import java.util.Set;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout;

/**
 * Handles the failures.
 *
 * <p>Created by zlokhandwala on 11/21/16.
 */
public class PurgeFailurePolicy implements IFailureHandlerPolicy {

    /**
     * Modifies the layout by removing/purging the set failed nodes.
     *
     * @param originalLayout Original Layout which needs to be modified.
     * @param corfuRuntime   Connected runtime to attach to the new layout.
     * @param failedNodes    Set of all failed/defected servers.
     * @return The new and modified layout.
     * @throws LayoutModificationException Thrown if attempt to create an invalid layout.
     * @throws CloneNotSupportedException  Clone not supported for layout.
     */
    @Override
    public Layout generateLayout(Layout originalLayout, CorfuRuntime corfuRuntime, Set<String>
            failedNodes)
            throws LayoutModificationException, CloneNotSupportedException {
        LayoutWorkflowManager layoutManager = new LayoutWorkflowManager(originalLayout);
        Layout newLayout = layoutManager
                .removeLayoutServers(failedNodes)
                .removeSequencerServers(failedNodes)
                .removeLogunitServers(failedNodes)
                .build();
        newLayout.setRuntime(corfuRuntime);
        newLayout.setEpoch(newLayout.getEpoch() + 1);
        return newLayout;
    }
}
