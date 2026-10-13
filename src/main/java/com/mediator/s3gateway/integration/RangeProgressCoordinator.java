package com.mediator.s3gateway.integration;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class RangeProgressCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RangeProgressCoordinator.class);

    private static final int RUNNING_STATE = 12;
    private static final int COMPLETED_STATE = 3;

    private final RequestRegistry requests;
    private final ManagerRegistrationClient managerClient;

    /*
     * One entry for one logical RESTORE:
     * user + bucket + key + object version + operation.
     */
    private final ConcurrentHashMap<String, ActiveRestore> activeTransfers = new ConcurrentHashMap<>();

    @Value("${gateway.progress.minimum-percent-step:3}")
    private int minimumPercentStep;

    @Value("${gateway.progress.idle-timeout-minutes:15}")
    private long idleTimeoutMinutes;

    public RangeProgressCoordinator(
            RequestRegistry requests,
            ManagerRegistrationClient managerClient) {
        this.requests = requests;
        this.managerClient = managerClient;
    }

    public RestoreSession getOrCreate(
            String bucket,
            String key,
            String etag,
            long totalSize) {

        String identity = sha256(
                "|" + bucket + "|" + key + "|"
                        + (etag == null ? "" : etag)
                        + "|RESTORE");

        ActiveRestore transfer = activeTransfers.compute(
                identity,
                (ignored, existing) -> {
                    if (existing != null && !existing.isExpired(idleTimeoutMinutes)) {
                        return existing;
                    }

                    int reqID = requests.submit(
                            "RESTORE",
                            bucket,
                            key,
                            totalSize,
                            Map.of(),
                            Map.of());

                    log.info(
                            "Created in-memory RESTORE session: reqID={}, bucket={}, key={}",
                            reqID, bucket, key);

                    return new ActiveRestore(
                            identity,
                            reqID,
                            totalSize,
                            etag == null ? "" : etag);
                });

        return new RestoreSession(
                transfer.identity,
                transfer.reqID,
                transfer.totalSize,
                false);
    }

    /*
     * Called by DownloadProgressInputStream.
     *
     * Important: no Manager HTTP call here.
     * This stays fast and runs on the S3 response-stream path.
     */
    public void recordDeliveredRange(
            RestoreSession session,
            long rangeStart,
            long rangeEnd) {

        if (rangeStart < 0 || rangeEnd < rangeStart) {
            return;
        }

        ActiveRestore transfer = activeTransfers.get(session.identity());

        if (transfer == null) {
            log.warn(
                    "Ignoring delivered range for expired session: reqID={}",
                    session.reqID());
            return;
        }

        synchronized (transfer) {
            transfer.mergeRange(rangeStart, rangeEnd);
            transfer.lastActivity = Instant.now();

            long deliveredBytes = transfer.deliveredBytes();
            int progress = calculateProgress(
                    deliveredBytes,
                    transfer.totalSize);

            if (deliveredBytes >= transfer.totalSize) {
                transfer.desiredProgress = 100;
                transfer.completionRequired = true;
            } else {
                transfer.desiredProgress = Math.min(99, progress);
            }
        }
    }

    /*
     * Runs outside all S3 GET/range request threads.
     * Coalesces progress: only the newest percentage is sent.
     */
    @Scheduled(fixedDelayString = "${gateway.progress.flush-interval-ms:5000}")
    public void flushPendingProgress() {

        activeTransfers.forEach((identity, transfer) -> {
            if (transfer.isExpired(idleTimeoutMinutes)
                    && !transfer.sendInFlight.get()) {

                activeTransfers.remove(identity, transfer);

                log.info(
                        "Expired inactive RESTORE session: reqID={}",
                        transfer.reqID);

                return;
            }

            sendLatestProgress(transfer);
        });
    }

    private void sendLatestProgress(ActiveRestore transfer) {
        int progressToSend;
        boolean terminal;

        synchronized (transfer) {
            if (transfer.sendInFlight.get()) {
                return;
            }

            terminal = transfer.completionRequired;

            if (terminal) {
                progressToSend = 100;
            } else {
                progressToSend = transfer.desiredProgress;

                if (progressToSend <= transfer.lastSentProgress) {
                    return;
                }

                if (progressToSend - transfer.lastSentProgress < minimumPercentStep) {
                    return;
                }
            }

            transfer.sendInFlight.set(true);
        }

        /*
         * This code runs on the scheduler thread, never on the GET stream.
         * If the Manager is unavailable, retain the in-memory state and retry
         * at the next scheduler execution.
         */
        try {
            if (terminal) {
                managerClient.setProgress(
                        transfer.reqID,
                        COMPLETED_STATE,
                        100,
                        "",
                        transfer.checksum);

                synchronized (transfer) {
                    transfer.lastSentProgress = 100;
                    transfer.completed = true;
                }

                activeTransfers.remove(transfer.identity, transfer);

                log.info(
                        "RESTORE completed: reqID={}",
                        transfer.reqID);

            } else {
                managerClient.setProgress(
                        transfer.reqID,
                        RUNNING_STATE,
                        progressToSend,
                        "",
                        "");

                synchronized (transfer) {
                    transfer.lastSentProgress = progressToSend;
                }

                log.debug(
                        "Asynchronous RESTORE progress sent: reqID={}, progress={}",
                        transfer.reqID,
                        progressToSend);
            }

        } catch (RuntimeException e) {
            /*
             * Do not clear desiredProgress or completionRequired.
             * The next scheduler execution retries the newest state.
             */
            log.warn(
                    "Manager progress update failed; will retry: reqID={}, progress={}",
                    transfer.reqID,
                    progressToSend,
                    e);

        } finally {
            transfer.sendInFlight.set(false);
        }
    }

    private static int calculateProgress(
            long deliveredBytes,
            long totalSize) {

        if (totalSize <= 0) {
            return 0;
        }

        return (int) Math.min(
                100,
                (deliveredBytes * 100L) / totalSize);
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest
                    .getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));

            StringBuilder result = new StringBuilder();

            for (byte valueByte : digest) {
                result.append(String.format("%02x", valueByte));
            }

            return result.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static final class ActiveRestore {

        private final String identity;
        private final int reqID;
        private final long totalSize;
        private final String checksum;

        /*
         * start byte -> inclusive end byte.
         * It always holds merged, non-overlapping intervals.
         */
        private final NavigableMap<Long, Long> deliveredRanges = new TreeMap<>();

        private final AtomicBoolean sendInFlight = new AtomicBoolean(false);

        private volatile Instant lastActivity = Instant.now();

        private int desiredProgress;
        private int lastSentProgress;
        private boolean completionRequired;
        private boolean completed;

        private ActiveRestore(
                String identity,
                int reqID,
                long totalSize,
                String checksum) {
            this.identity = identity;
            this.reqID = reqID;
            this.totalSize = totalSize;
            this.checksum = checksum;
        }

        private void mergeRange(long start, long end) {
            Map.Entry<Long, Long> previous = deliveredRanges.floorEntry(start);

            if (previous != null && previous.getValue() + 1 >= start) {
                start = previous.getKey();
                end = Math.max(end, previous.getValue());
                deliveredRanges.remove(previous.getKey());
            }

            Map.Entry<Long, Long> next = deliveredRanges.ceilingEntry(start);

            while (next != null && next.getKey() <= end + 1) {
                end = Math.max(end, next.getValue());
                deliveredRanges.remove(next.getKey());
                next = deliveredRanges.ceilingEntry(start);
            }

            deliveredRanges.put(start, end);
        }

        private long deliveredBytes() {
            return deliveredRanges.entrySet().stream()
                    .mapToLong(entry -> entry.getValue() - entry.getKey() + 1)
                    .sum();
        }

        private boolean isExpired(long timeoutMinutes) {
            return !completionRequired
                    && Duration.between(lastActivity, Instant.now())
                            .toMinutes() >= timeoutMinutes;
        }
    }
}