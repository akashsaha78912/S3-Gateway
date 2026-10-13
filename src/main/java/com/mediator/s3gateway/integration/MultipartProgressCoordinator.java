package com.mediator.s3gateway.integration;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class MultipartProgressCoordinator {

    private static final Logger log = LoggerFactory.getLogger(MultipartProgressCoordinator.class);

    private static final int RUNNING_STATE = 12;

    private final ManagerRegistrationClient managerClient;

    private final ConcurrentHashMap<String, UploadState> uploads = new ConcurrentHashMap<>();

    @Value("${gateway.progress.minimum-percent-step:3}")
    private int minimumPercentStep;

    public MultipartProgressCoordinator(
            ManagerRegistrationClient managerClient) {
        this.managerClient = managerClient;
    }

    public void open(String uploadId, int reqID) {
        uploads.putIfAbsent(uploadId, new UploadState(reqID));
    }

    public void startPart(
            String uploadId,
            int partNumber,
            long expectedBytes) {

        UploadState state = require(uploadId);

        synchronized (state) {
            state.inFlight.put(
                    partNumber,
                    new InFlightPart(expectedBytes, 0));
            state.lastActivity = Instant.now();
        }
    }

    public void bytesReceived(
            String uploadId,
            int partNumber,
            long receivedBytes) {

        UploadState state = uploads.get(uploadId);

        if (state == null) {
            return;
        }

        synchronized (state) {
            InFlightPart current = state.inFlight.get(partNumber);

            if (current == null) {
                return;
            }

            state.inFlight.put(
                    partNumber,
                    new InFlightPart(
                            current.expectedBytes(),
                            Math.min(receivedBytes, current.expectedBytes())));

            state.lastActivity = Instant.now();
        }
    }

    /*
     * Call only after MultipartUploadStore.putPart() succeeds.
     * A retried part replaces its prior accepted size.
     */
    public void partAccepted(
            String uploadId,
            int partNumber,
            long acceptedPartBytes) {

        UploadState state = require(uploadId);

        synchronized (state) {
            state.acceptedPartSizes.put(partNumber, acceptedPartBytes);
            state.inFlight.remove(partNumber);
            state.lastActivity = Instant.now();
        }
    }

    public void partFailed(String uploadId, int partNumber) {
        UploadState state = uploads.get(uploadId);

        if (state == null) {
            return;
        }

        synchronized (state) {
            state.inFlight.remove(partNumber);
            state.lastActivity = Instant.now();
        }
    }

    /*
     * This runs independently of UploadPart request threads.
     * Progress is rolling and capped at 99.
     */
    @Scheduled(fixedDelayString = "${gateway.progress.flush-interval-ms:2000}")
    public void flushRollingProgress() {

        uploads.forEach((uploadId, state) -> {
            int progressToSend;

            synchronized (state) {
                progressToSend = state.rollingProgress();

                if (state.lastSentProgress >= 0
                        && Math.abs(progressToSend - state.lastSentProgress) < minimumPercentStep) {
                    return;
                }
            }

            try {
                managerClient.setProgress(
                        state.reqID,
                        RUNNING_STATE,
                        progressToSend,
                        "",
                        "");

                synchronized (state) {
                    state.lastSentProgress = progressToSend;
                }

                log.debug(
                        "Multipart rolling progress sent: uploadId={}, reqID={}, progress={}%",
                        uploadId,
                        state.reqID,
                        progressToSend);

            } catch (RuntimeException error) {
                /*
                 * Keep state in memory. The scheduler retries the latest
                 * rolling value on its next run.
                 */
                log.warn(
                        "Multipart progress send failed; retrying later: uploadId={}, reqID={}",
                        uploadId,
                        state.reqID,
                        error);
            }
        });
    }

    public void remove(String uploadId) {
        uploads.remove(uploadId);
    }

    private UploadState require(String uploadId) {
        UploadState state = uploads.get(uploadId);

        if (state == null) {
            throw new IllegalStateException(
                    "Multipart progress was not initialized: uploadId="
                            + uploadId);
        }

        return state;
    }

    private static final class UploadState {

        private final int reqID;

        /*
         * Only validated, successfully persisted parts belong here.
         * key=partNumber, value=part byte count.
         */
        private final Map<Integer, Long> acceptedPartSizes = new HashMap<>();

        /*
         * Parts currently being received.
         */
        private final Map<Integer, InFlightPart> inFlight = new HashMap<>();

        private int lastSentProgress = -1;
        private Instant lastActivity = Instant.now();

        private UploadState(int reqID) {
            this.reqID = reqID;
        }

        private int rollingProgress() {
            long acceptedBytes = acceptedPartSizes.values().stream()
                    .mapToLong(Long::longValue)
                    .sum();

            long inFlightBytes = inFlight.values().stream()
                    .mapToLong(InFlightPart::receivedBytes)
                    .sum();

            long inFlightExpectedBytes = inFlight.values().stream()
                    .mapToLong(InFlightPart::expectedBytes)
                    .sum();

            long denominator = acceptedBytes + inFlightExpectedBytes;

            if (denominator == 0) {
                return 0;
            }

            return (int) Math.min(
                    99,
                    ((acceptedBytes + inFlightBytes) * 100L)
                            / denominator);
        }
    }

    private record InFlightPart(
            long expectedBytes,
            long receivedBytes) {
    }
}