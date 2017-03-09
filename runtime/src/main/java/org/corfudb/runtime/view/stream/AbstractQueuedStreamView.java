package org.corfudb.runtime.view.stream;

import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** The abstract queued stream view implements a stream backed by a read queue.
 *
 * A read queue is a priority queue where addresses can be inserted, and are
 * dequeued in ascending order. Subclasses implement the fillReadQueue()
 * function, which defines how the read queue should be filled, and the
 * read() function, which reads an entry and updates the pointers for the
 * stream view.
 *
 * The addresses in the read queue must be global addresses.
 *
 * This implementation does not handle bulk reads and depends on IStreamView's
 * implementation of remainingUpTo(), which simply calls nextUpTo() under a lock
 * until it returns null.
 *
 * Created by mwei on 1/6/17.
 */
@Slf4j
public abstract class AbstractQueuedStreamView extends
        AbstractContextStreamView<AbstractQueuedStreamView
                .QueuedStreamContext> {

    /** Create a new queued stream view.
     *
     * @param streamID  The ID of the stream
     * @param runtime   The runtime used to create this view.
     */
    public AbstractQueuedStreamView(final CorfuRuntime runtime,
                                    final UUID streamID) {
        super(runtime, streamID, QueuedStreamContext::new);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ILogData getNextEntry(QueuedStreamContext context,
                                    long maxGlobal) {
        // If we have no entries to read, fill the read queue.
        // Return if the queue is still empty.
        if (context.readQueue.isEmpty() &&
                !fillReadQueue(maxGlobal, context)) {
            return null;
        }

        // If the lowest element is greater than maxGlobal, there's nothing
        // to return.
        if (context.readQueue.first() > maxGlobal) {
            return null;
        }

        // Otherwise we remove entries one at a time from the read queue.
        // The entry may not actually be part of the stream, so we might
        // have to perform several reads.
        while (context.readQueue.size() > 0) {
            final long thisRead = context.readQueue.pollFirst();
            ILogData ld = read(thisRead);
            if (ld.containsStream(context.id)) {
                if (context.maxResolution < thisRead)
                {
                    context.resolvedQueue.add(thisRead);
                    context.maxResolution = thisRead;
                }
                return ld;
            }
        }

        // None of the potential reads ended up being part of this
        // stream, so we return null.
        return null;
    }

    /** {@inheritDoc}
     *
     * In the queued implementation, we just read all entries in the read queue
     * in parallel. If there is any entry which changes the context, we cut the
     * list off there.
     * */
    @Override
    protected List<ILogData> getNextEntries(QueuedStreamContext context, long maxGlobal,
                                            Function<ILogData, Boolean> contextCheckFn) {
        // We always have to fill to the read queue to ensure we read up to
        // max global.
        if (!fillReadQueue(maxGlobal, context)) {
            return Collections.emptyList();
        }

        // If the lowest element is greater than maxGlobal, there's nothing
        // to return.
        if (context.readQueue.first() > maxGlobal) {
            return Collections.emptyList();
        }

        // The list to store read results in
        List<ILogData> read = new ArrayList<>();

        // While we have data and haven't exceeded maxGlobal
        while (context.readQueue.size() > 0 &&
                context.readQueue.first() <= maxGlobal) {

            // Do the read, removing the request from the queue
            long readAddress = context.readQueue.pollFirst();
            ILogData data = read(readAddress);

            // Add to the read list if the entry is part of this
            // stream and contains data
            if (data.getType() == DataType.DATA &&
                    data.containsStream(context.id)) {

                if (context.maxResolution < readAddress)
                {
                    context.resolvedQueue.add(readAddress);
                    context.maxResolution = readAddress;
                }

                read.add(data);
            }

            // Update the pointer.
            context.globalPointer = readAddress;

            // If the context changed, return
            if (contextCheckFn.apply(data)) {
                return read;
            }
        }

        // Return the list of entries read.
        return read;
    }

    /**
     * Retrieve the data at the given address which was previously
     * inserted into the read queue.
     *
     * @param address       The address to read.
     */
    abstract protected @NonNull ILogData read(final long address);

    /**
     * Fill the read queue for the current context. This method is called
     * whenever a client requests a read, but there are no addresses left in
     * the read queue.
     *
     * This method returns true if entries were added to the read queue,
     * false otherwise.
     *
     * @param maxGlobal     The maximum global address to read to.
     * @param context       The current stream context.
     *
     * @return              True, if entries were added to the read queue,
     *                      False, otherwise.
     */
    abstract protected boolean fillReadQueue(final long maxGlobal,
                                          final QueuedStreamContext context);


    /** {@inheritDoc}
     *
     * For the queued stream context, we include just a queue of potential
     * global addresses to be read from.
     */
    @ToString
    static class QueuedStreamContext extends AbstractStreamContext {


        /** A queue of addresses which have already been resolved. */
        final NavigableSet<Long> resolvedQueue
                = new TreeSet<>();

        /** The maximum global address which we have resolved this
         * stream to.
         */
        long maxResolution = Address.NEVER_READ;

        /**
         * A priority queue of potential addresses to be read from.
         */
        final NavigableSet<Long> readQueue
                = new TreeSet<>();

        /** Create a new stream context with the given ID and maximum address
         * to read to.
         * @param id                  The ID of the stream to read from
         * @param maxGlobalAddress    The maximum address for the context.
         */
        public QueuedStreamContext(UUID id, long maxGlobalAddress) {
            super(id, maxGlobalAddress);
        }


        /** {@inheritDoc} */
        @Override
        void reset() {
            super.reset();
            readQueue.clear();
        }
    }

}
