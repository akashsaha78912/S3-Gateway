package com.mediator.s3gateway.web;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Strips AWS SigV4 chunk headers ("<hex-size>;chunk-signature=...\r\n") 
 * from STREAMING-AWS4-HMAC-SHA256-PAYLOAD streams without requiring credential verification.
 */
public class AwsChunkedInputStream extends FilterInputStream {

    private long currentChunkRemaining = 0;
    private boolean isEof = false;

    public AwsChunkedInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int n = read(b, 0, 1);
        return n == -1 ? -1 : (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (isEof) {
            return -1;
        }

        if (currentChunkRemaining == 0) {
            readNextChunkHeader();
            if (isEof) {
                return -1;
            }
        }

        int bytesToRead = (int) Math.min(len, currentChunkRemaining);
        int bytesRead = super.read(b, off, bytesToRead);

        if (bytesRead > 0) {
            currentChunkRemaining -= bytesRead;
            if (currentChunkRemaining == 0) {
                // Consume trailing CRLF (\r\n) after chunk data
                readCRLF();
            }
        }

        return bytesRead;
    }

    private void readNextChunkHeader() throws IOException {
        StringBuilder header = new StringBuilder();
        int ch;
        while ((ch = super.read()) != -1) {
            if (ch == '\r') {
                int next = super.read(); // Read matching '\n'
                break;
            }
            header.append((char) ch);
        }

        String headerLine = header.toString().trim();
        if (headerLine.isEmpty()) {
            isEof = true;
            return;
        }

        // Chunk header format: "10000;chunk-signature=..."
        String hexSize = headerLine.split(";")[0].trim();
        try {
            currentChunkRemaining = Long.parseLong(hexSize, 16);
            if (currentChunkRemaining == 0) {
                isEof = true;
                // Read trailing empty CRLF
                readCRLF();
            }
        } catch (NumberFormatException e) {
            throw new IOException("Malformed aws-chunked size header: " + hexSize, e);
        }
    }

    private void readCRLF() throws IOException {
        int r = super.read();
        if (r == '\r') {
            super.read(); // consume \n
        }
    }
}