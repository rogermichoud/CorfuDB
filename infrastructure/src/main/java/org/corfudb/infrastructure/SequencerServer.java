package org.corfudb.infrastructure;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;

/**
 * This server implements the sequencer functionality of Corfu.
 *
 * <p>It currently supports a single operation, which is a incoming request:
 *
 * <p>TOKEN_REQ - Request the next address.
 *
 * <p>The sequencer server maintains the current tail of the log, the current
 * tail of every stream, and a cache of timestamps of updates on recent
 * conflict-parameters.
 *
 * <p>A token request can be of several sub-types, which are defined in
 * {@link TokenRequest}:
 *
 * <p>{@link TokenRequest::TK_QUERY} - used for only querying the current tail
 * of the log and/or the tails of specific streams
 *
 * <p>{@link TokenRequest::TK_RAW} - reserved for getting a "raw" token in the
 * global log
 *
 * <p>{@link TokenRequest::TK_MULTI_STREAM} - used for logging across one or
 * more streams
 *
 * <p>{@link TokenRequest::TK_TX} - used for reserving an address for transaction
 * commit.
 *
 * <p>The transaction commit is the most sophisticated functaionality of the
 * sequencer. The sequencer reserves an address for the transaction
 * only on if it knows that it can commit.
 *
 * <p>The TK_TX request contains a conflict-set and a write-set. The sequencer
 * checks the conflict-set against the stream-tails and against the
 * conflict-parameters timestamp cache it maintains. If the transaction
 * commits, the sequencer updates the tails of all the streams and the cache
 * of conflict parameters.
 *
 * <p>Created by mwei on 12/8/15.
 */
@Slf4j
public class SequencerServer extends AbstractServer {

    /**
     * key-name for storing {@link SequencerServer} state in {@link ServerContext::getDataStore()}.
     */
    private static final String PREFIX_SEQUENCER = "SEQUENCER";

    /**
     * Inherit from CorfuServer a server context.
     */
    private final ServerContext serverContext;

    /**
     * Our options.
     */
    private final Map<String, Object> opts;

    /**
     * - {@link SequencerServer::globalLogTail}:
     * global log first available position (initially, 0).
     */
    @Getter
    private final AtomicLong globalLogTail = new AtomicLong(Address
            .getMinAddress());

    /**
     * remember start point, if sequencer is started as failover sequencer.
     */
    private final AtomicLong globalLogStart = new AtomicLong(Address
            .getMinAddress());

    /**
     * - {@link SequencerServer::streamTailToGlobalTailMap}:
     * per streams map to last issued global-log position. used for
     * backpointers.
     */
    private final ConcurrentHashMap<UUID, Long> streamTailToGlobalTailMap = new
            ConcurrentHashMap<>();

    /**
     * TX conflict-resolution information:
     *
     * {@link SequencerServer::conflictToGlobalTailCache}:
     * a cache of recent conflict keys and their latest global-log
     * position.
     *
     * {@link SequencerServer::maxConflictWildcard} :
     * a "wildcard" representing the maximal update timestamp of
     * all the confict keys which were evicted from the cache
     */
    private long maxConflictWildcard = Address.NOT_FOUND;

    private final long defaultCacheSize = Long.MAX_VALUE;
    private final Cache<Integer, Long> conflictToGlobalTailCache;

    /**
     * Handler for this server.
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    private static final String metricsPrefix = "corfu.server.sequencer.";
    private static Counter counterTokenSum;
    private static Counter counterToken0;

    @Override
    public boolean isServerReady() {
        return serverContext.isReady();
    }

    /**
     * Returns a new SequencerServer.
     * @param serverContext context object providing parameters and objects
     */
    public SequencerServer(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.opts = serverContext.getServerConfig();

        long initialToken = Utils.parseLong(opts.get("--initial-token"));
        if (Address.nonAddress(initialToken)) {
            globalLogTail.set(0L);
        } else {
            globalLogTail.set(initialToken);
        }

        MetricRegistry metrics = serverContext.getMetrics();
        counterTokenSum = metrics.counter(metricsPrefix + "token-sum");
        counterToken0 = metrics.counter(metricsPrefix + "token-query");

        long cacheSize = defaultCacheSize;

        if (opts.get("--sequencer-cache-size") != null
                && Long.parseLong((String) opts.get("--sequencer-cache-size")) != 0) {
            cacheSize = Long.parseLong((String) opts.get("--sequencer-cache-size"));
        }

        conflictToGlobalTailCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .removalListener((Integer k, Long v, RemovalCause cause) -> {
                    if (!RemovalCause.REPLACED.equals(cause)) {
                        log.trace("Updating maxConflictWildcard. Old value = '{}', new value='{}'"
                                        + " conflictParam = '{}'. Removal cause = '{}'",
                                maxConflictWildcard, v, k, cause);
                        maxConflictWildcard = Math.max(v, maxConflictWildcard);
                    }
                })
                .recordStats()
                .build();
    }

    /**
    * Get the conflict hash code for a stream ID and conflict param.
    *
    * @param streamId      The stream ID.
    * @param conflictParam The conflict parameter.
    * @return A conflict hash code.
    */
    public int getConflictHashCode(UUID streamId, int conflictParam) {
        return Objects.hash(streamId, conflictParam);
    }

    /**
     * If the request submits a timestamp (a global offset) that is less than one of the
     * global offsets of a streams specified in the request, then abort; otherwise commit.
     *
     * @param txInfo      info provided by corfuRuntime for conflict resolultion:
     *                    - timestamp : the snapshot (global) offset that this TX reads
     *                    - conflictSet: conflict set of the txn.
     *                    if any conflict-param (or stream, if empty) in this set has a later
     *                    timestamp than the snapshot, abort
     * @param conflictKey is a return parameter that signals to the consumer which key was
     *                    responsible for unsuccessful allocation af a token.
     * @return Returns the type of token reponse based on whether the txn commits, or the abort
     *     cause.
     */
    public TokenType txnCanCommit(TxResolutionInfo txInfo, /** Input. */
                                  AtomicReference<Integer> conflictKey /** Output. */) {
        log.trace("Commit-req[{}]", txInfo);
        final long txSnapshotTimestamp = txInfo.getSnapshotTimestamp();

        if (txSnapshotTimestamp < globalLogStart.get() - 1) {
            log.debug("ABORT[{}] snapshot-ts[{}] failover-ts[{}]",
                    txSnapshotTimestamp, globalLogStart.get());
            return TokenType.TX_ABORT_NEWSEQ;
        }

        AtomicReference<TokenType> response = new AtomicReference<>(TokenType.NORMAL);

        for (Map.Entry<UUID, Set<Integer>> entry : txInfo.getConflictSet().entrySet()) {
            if (response.get() != TokenType.NORMAL) {
                break;
            }

            // if conflict-parameters are present, check for conflict based on conflict-parameter
            // updates
            Set<Integer> conflictParamSet = entry.getValue();
            if (conflictParamSet != null && conflictParamSet.size() > 0) {
                // for each key pair, check for conflict;
                // if not present, check against the wildcard
                conflictParamSet.forEach(conflictParam -> {
                    int conflictKeyHash = getConflictHashCode(entry.getKey(),
                            conflictParam);
                    Long v = conflictToGlobalTailCache.getIfPresent(conflictKeyHash);

                    log.trace("Commit-ck[{}] conflict-key[{}](ts={})", txInfo, conflictParam, v);

                    if (v != null && v > txSnapshotTimestamp) {
                        log.debug("ABORT[{}] conflict-key[{}](ts={})", txInfo, conflictParam, v);
                        conflictKey.set(conflictParam);
                        response.set(TokenType.TX_ABORT_CONFLICT);
                    }

                    if (v == null && maxConflictWildcard > txSnapshotTimestamp) {
                        log.warn("ABORT[{}] conflict-key[{}](WILDCARD ts={})", txInfo,
                                conflictParam,
                                maxConflictWildcard);
                        conflictKey.set(conflictParam);
                        response.set(TokenType.TX_ABORT_CONFLICT);
                    }
                });
            } else { // otherwise, check for conflict based on streams updates
                UUID streamId = entry.getKey();
                streamTailToGlobalTailMap.compute(streamId, (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    if (v > txSnapshotTimestamp) {
                        log.debug("ABORT[{}] conflict-stream[{}](ts={})",
                                txInfo, Utils.toReadableId(streamId), v);
                        response.set(TokenType.TX_ABORT_CONFLICT);
                    }
                    return v;
                });
            }
        }

        return response.get();
    }

    /**
     * Service a query request.
     *
     * <p>This returns information about the tail of the
     * log and/or streams without changing/allocating anything.
     *
     * @param msg corfu message containing token query
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    public void handleTokenQuery(CorfuPayloadMsg<TokenRequest> msg,
                                 ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();

        // sanity backward-compatibility assertion; TODO: remove
        if (req.getStreams().size() > 1) {
            log.error("TOKEN-QUERY[{}]", req.getStreams());
        }

        long maxStreamGlobalTail = Address.NON_EXIST;

        // see if this query is for a specific stream-tail
        if (req.getStreams().size() == 1) {
            UUID streamId = req.getStreams().iterator().next();

            if (streamTailToGlobalTailMap.get(streamId) != null) {
                maxStreamGlobalTail = streamTailToGlobalTailMap.get(streamId);
            }
        }

        // If no streams are specified in the request, this value returns the last global token
        // issued.
        long responseGlobalTail = (req.getStreams().size() == 0) ? globalLogTail.get() - 1 :
                maxStreamGlobalTail;
        Token token = new Token(responseGlobalTail, r.getServerEpoch());
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY, token, Collections.emptyMap())));
    }

    /**
     * Service an incoming request to reset the sequencer.
     */
    @ServerHandler(type = CorfuMsgType.RESET_SEQUENCER, opTimer = metricsPrefix + "reset")
    public synchronized void resetServer(CorfuPayloadMsg<SequencerTailsRecoveryMsg> msg,
                                         ChannelHandlerContext ctx, IServerRouter r,
                                         boolean isMetricsEnabled) {
        long initialToken = msg.getPayload().getGlobalTail();
        final Map<UUID, Long> streamTails = msg.getPayload().getStreamTails();

        //
        // if the sequencer is reset, then we can't know when was
        // the latest update to any stream or conflict parameter.
        // hence, we want to conservatively abort any transaction with snapshot time
        // preceding the reset-time of this sequencer.
        //
        // Therefore, we remember the new start tail.
        // We empty the cache of conflict parameters.
        // We set the wildcard to the new start tail.
        //
        // Note, this is correct, but conservative (may lead to false abort).
        // It is necessary because we reset the sequencer.
        //
        if (initialToken > globalLogTail.get()) {
            globalLogTail.set(initialToken);
            globalLogStart.set(initialToken);
            maxConflictWildcard = initialToken - 1;
            conflictToGlobalTailCache.invalidateAll();

            // Clear the existing map as it could have been populated by an earlier reset.
            streamTailToGlobalTailMap.clear();
            streamTailToGlobalTailMap.putAll(streamTails);
        }

        log.info("Sequencer reset with token = {}, streamTailToGlobalTailMap = {}",
                initialToken, streamTailToGlobalTailMap);
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Service an incoming token request.
     */
    @ServerHandler(type = CorfuMsgType.TOKEN_REQ, opTimer = metricsPrefix + "token-req")
    public synchronized void tokenRequest(CorfuPayloadMsg<TokenRequest> msg,
                                          ChannelHandlerContext ctx, IServerRouter r,
                                          boolean isMetricsEnabled) {
        TokenRequest req = msg.getPayload();

        // metrics collection
        if (req.getReqType() == TokenRequest.TK_QUERY) {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterToken0, 1);
        } else {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterTokenSum, req
                    .getNumTokens());
        }

        // dispatch request handler according to request type
        switch (req.getReqType()) {
            case TokenRequest.TK_QUERY:
                handleTokenQuery(msg, ctx, r);
                return;

            case TokenRequest.TK_RAW:
                handleRawToken(msg, ctx, r);
                return;

            case TokenRequest.TK_TX:
                handleTxToken(msg, ctx, r);
                return;

            default:
                handleAllocation(msg, ctx, r);
                return;
        }
    }

    /**
     * this method serves log-tokens for a raw log implementation.
     * it simply extends the global log tail and returns the global-log token
     *
     * @param msg corfu message containing raw token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleRawToken(CorfuPayloadMsg<TokenRequest> msg,
                                ChannelHandlerContext ctx, IServerRouter r) {
        final long serverEpoch = r.getServerEpoch();
        final TokenRequest req = msg.getPayload();

        Token token = new Token(globalLogTail.getAndAdd(req.getNumTokens()), serverEpoch);
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY, token, Collections.emptyMap())));

    }

    /**
     * this method serves token-requests for transaction-commit entries.
     *
     * <p>it checks if the transaction can commit.
     * - if the transction must abort,
     * then a 'error token' containing an Address.ABORTED address is returned.
     * - if the transaction may commit,
     * then a normal allocation of log position(s) is pursued.
     *
     * @param msg corfu message containing transaction token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleTxToken(CorfuPayloadMsg<TokenRequest> msg,
                               ChannelHandlerContext ctx, IServerRouter r) {
        final long serverEpoch = r.getServerEpoch();
        final TokenRequest req = msg.getPayload();

        // Since Java does not allow an easy way for a function to return multiple values, this
        // variable is passed to the consumer that will use it to indicate to us if/what key was
        // responsible for an aborted transaction.
        AtomicReference<Integer> conflictKey = new AtomicReference(TokenResponse.NO_CONFLICT_KEY);

        // in the TK_TX request type, the sequencer is utilized for transaction conflict-resolution.
        // Token allocation is conditioned on commit.
        // First, we check if the transaction can commit.
        TokenType tokenType = txnCanCommit(req.getTxnResolution(), conflictKey);
        if (tokenType != TokenType.NORMAL) {
            // If the txn aborts, then DO NOT hand out a token.
            Token token = new Token(Address.ABORTED, serverEpoch);
            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(tokenType,
                    conflictKey.get(), token, Collections.emptyMap())));
            return;
        }

        // if we get here, this means the transaction can commit.
        // handleAllocation() does the actual allocation of log position(s)
        // and returns the reponse
        handleAllocation(msg, ctx, r);
    }

    /**
     * this method does the actual allocation of log addresses,
     * it also maintains stream-tails, returns a map of stream-tails for backpointers,
     * and maintains a conflict-parameters map.
     *
     * @param msg corfu message containing allocation
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleAllocation(CorfuPayloadMsg<TokenRequest> msg,
                                  ChannelHandlerContext ctx, IServerRouter r) {
        final long serverEpoch = r.getServerEpoch();
        final TokenRequest req = msg.getPayload();

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        long currentTail = globalLogTail.getAndAdd(req.getNumTokens());
        long newTail = currentTail + req.getNumTokens();

        // for each streams:
        //   1. obtain the last back-pointer for this streams, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this streams.
        //   3. extend the tail by the requested # tokens.
        ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
        for (UUID id : req.getStreams()) {

            // step 1. and 2. (comment above)
            streamTailToGlobalTailMap.compute(id, (k, v) -> {
                if (v == null) {
                    backPointerMap.put(k, Address.NON_EXIST);
                    return newTail - 1;
                } else {
                    backPointerMap.put(k, v);
                    return newTail - 1;
                }
            });
        }

        // update the cache of conflict parameters
        if (req.getTxnResolution() != null) {
            req.getTxnResolution().getWriteConflictParams().entrySet()
                    .stream()
                    // for each entry
                    .forEach(txEntry ->
                            // and for each conflict param
                            txEntry.getValue().stream().forEach(conflictParam ->
                                // insert an entry with the new timestamp
                                // using the hash code based on the param
                                // and the stream id.
                                conflictToGlobalTailCache.put(
                                        getConflictHashCode(txEntry
                                        .getKey(), conflictParam),
                                    newTail - 1)));
        }

        log.trace("token {} backpointers {}",
                currentTail, backPointerMap.build());
        // return the token response with the new global tail
        // and the streams backpointers
        Token token = new Token(currentTail, serverEpoch);
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY, token,
                backPointerMap.build())));
    }
}
