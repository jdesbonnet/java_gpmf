package ie.strix.gpmf.klv;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class GpmfParser {

    public static final class Entry {
        public final String fourCC;
        public final int type;
        public final int structSize;
        public final int repeat;
        public final long dataSize;
        public final byte[] payload;
        public final List<Entry> children;

        private Entry(String fourCC, int type, int structSize, int repeat,
                      long dataSize, byte[] payload, List<Entry> children) {
            this.fourCC = fourCC;
            this.type = type;
            this.structSize = structSize;
            this.repeat = repeat;
            this.dataSize = dataSize;
            this.payload = payload;
            this.children = children;
        }

        public boolean isNested() {
            return type == 0;
        }
    }

    public interface EntryHandler {
        void onEntry(Entry entry, int depth);
    }

    public static final class Options {
        public boolean capturePayload = true;
        public long maxPayloadBytes = 16L * 1024L * 1024L;
        public boolean parseNested = true;
    }

    private GpmfParser() {
    }

    public static List<Entry> parseToTree(InputStream in, Options opt) throws IOException {
        Objects.requireNonNull(in);
        Objects.requireNonNull(opt);
        List<Entry> out = new ArrayList<>();
        parseStream(in, opt, (entry, depth) -> {
            if (depth == 0) {
                out.add(entry);
            }
        }, 0, Long.MAX_VALUE, true);
        return out;
    }

    public static void parseStreaming(InputStream in, Options opt, EntryHandler handler) throws IOException {
        Objects.requireNonNull(in);
        Objects.requireNonNull(opt);
        Objects.requireNonNull(handler);
        parseStream(in, opt, handler, 0, Long.MAX_VALUE, false);
    }

    public static void parseStreamingBounded(InputStream in, Options opt, long bytesLimit, EntryHandler handler)
            throws IOException {
        Objects.requireNonNull(in);
        Objects.requireNonNull(opt);
        Objects.requireNonNull(handler);
        parseStream(in, opt, handler, 0, bytesLimit, false);
    }

    private static void parseStream(InputStream in,
                                    Options opt,
                                    EntryHandler handler,
                                    int depth,
                                    long bytesLimit,
                                    boolean collectChildren) throws IOException {

        CountingInputStream cin = (in instanceof CountingInputStream counting)
                ? counting
                : new CountingInputStream(in);

        long startPos = cin.count;
        while (true) {
            long consumed = cin.count - startPos;
            if (consumed >= bytesLimit) {
                break;
            }

            if (!ensureAvailable(cin, bytesLimit, startPos, 8)) {
                break;
            }

            int key = readU32BE(cin);
            int lenField = readU32BE(cin);

            String fourCC = fourCC(key);
            int type = (lenField >>> 24) & 0xFF;
            int structSize = (lenField >>> 16) & 0xFF;
            int repeat = lenField & 0xFFFF;

            long dataSize = (long) structSize * (long) repeat;
            long storedSize = align4(dataSize);

            if (!ensureAvailable(cin, bytesLimit, startPos, storedSize)) {
                throw new EOFException("Truncated payload for " + fourCC + " (need " + storedSize + " bytes)");
            }

            byte[] payload = null;
            List<Entry> children = null;
            boolean wantPayload = opt.capturePayload && dataSize <= opt.maxPayloadBytes;

            if (type == 0 && opt.parseNested) {
                LimitedInputStream nestedSlice = new LimitedInputStream(cin, dataSize);
                if (collectChildren) {
                    children = new ArrayList<>();
                }
                final List<Entry> childSink = children;
                parseStream(nestedSlice, opt, (child, childDepth) -> {
                    if (collectChildren) {
                        childSink.add(child);
                    }
                    handler.onEntry(child, childDepth);
                }, depth + 1, dataSize, collectChildren);
                nestedSlice.drain();
                skipFully(cin, storedSize - dataSize);
            } else {
                if (wantPayload) {
                    payload = readFullyBytes(cin, (int) dataSize);
                } else {
                    skipFully(cin, dataSize);
                }
                skipFully(cin, storedSize - dataSize);
            }

            handler.onEntry(new Entry(fourCC, type, structSize, repeat, dataSize, payload, children), depth);
        }
    }

    private static long align4(long n) {
        return (n + 3L) & ~3L;
    }

    private static boolean ensureAvailable(CountingInputStream cin, long bytesLimit, long startPos, long need)
            throws IOException {
        long consumed = cin.count - startPos;
        long remaining = bytesLimit - consumed;
        if (remaining < need) {
            return false;
        }
        if (need == 0) {
            return true;
        }
        int b = cin.read();
        if (b < 0) {
            return false;
        }
        cin.unread(b);
        return true;
    }

    private static int readU32BE(InputStream in) throws IOException {
        int b0 = in.read();
        int b1 = in.read();
        int b2 = in.read();
        int b3 = in.read();
        if ((b0 | b1 | b2 | b3) < 0) {
            throw new EOFException("Unexpected EOF reading u32");
        }
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    private static void skipFully(InputStream in, long n) throws IOException {
        long remaining = n;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped > 0) {
                remaining -= skipped;
                continue;
            }
            int b = in.read();
            if (b < 0) {
                throw new EOFException("Unexpected EOF while skipping " + n + " bytes");
            }
            remaining--;
        }
    }

    private static byte[] readFullyBytes(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            int r = in.read(buf, off, n - off);
            if (r < 0) {
                throw new EOFException("Unexpected EOF reading " + n + " bytes");
            }
            off += r;
        }
        return buf;
    }

    private static String fourCC(int v) {
        byte[] b = new byte[] {
                (byte) ((v >>> 24) & 0xFF),
                (byte) ((v >>> 16) & 0xFF),
                (byte) ((v >>> 8) & 0xFF),
                (byte) (v & 0xFF)
        };
        return new String(b, StandardCharsets.US_ASCII);
    }

    private static final class CountingInputStream extends InputStream {
        private final PushbackInputStream delegate;
        private long count;

        private CountingInputStream(InputStream in) {
            this.delegate = new PushbackInputStream(in, 1);
        }

        @Override
        public int read() throws IOException {
            int b = delegate.read();
            if (b >= 0) {
                count++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = delegate.read(b, off, len);
            if (n > 0) {
                count += n;
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = delegate.skip(n);
            if (skipped > 0) {
                count += skipped;
            }
            return skipped;
        }

        private void unread(int b) throws IOException {
            delegate.unread(b);
            count--;
        }
    }

    private static final class LimitedInputStream extends InputStream {
        private final CountingInputStream in;
        private long remaining;

        private LimitedInputStream(CountingInputStream in, long limit) {
            this.in = in;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = in.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int max = (int) Math.min(len, remaining);
            int n = in.read(b, off, max);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        private void drain() throws IOException {
            while (remaining > 0) {
                long skipped = skip(remaining);
                if (skipped <= 0) {
                    int b = read();
                    if (b < 0) {
                        break;
                    }
                }
            }
        }
    }
}
