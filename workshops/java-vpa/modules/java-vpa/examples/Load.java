import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Tiny load target: an HTTP server with knobs for heap, CPU and threads.
 * Runs as a single source file: `java Load.java` (JEP 330), no build.
 *
 *   GET /            -> JVM view of its own limits
 *   GET /alloc?mb=N  -> retain N MiB on the heap (cumulative)
 *   GET /free        -> drop retained memory
 *   GET /cpu?t=N&s=S -> spin N threads for S seconds
 */
public class Load {
    static final List<byte[]> HELD = new ArrayList<>();

    public static void main(String[] a) throws Exception {
        HttpServer s = HttpServer.create(new InetSocketAddress(8080), 0);
        s.setExecutor(Executors.newFixedThreadPool(8));
        s.createContext("/", x -> reply(x, info()));
        s.createContext("/alloc", x -> {
            int mb = param(x.getRequestURI().getQuery(), "mb", 64);
            synchronized (HELD) {
                for (int i = 0; i < mb; i++) HELD.add(new byte[1 << 20]);
            }
            reply(x, "retained " + mb + " MiB\n" + info());
        });
        s.createContext("/free", x -> {
            synchronized (HELD) { HELD.clear(); }
            System.gc();
            reply(x, "freed\n" + info());
        });
        s.createContext("/cpu", x -> {
            String q = x.getRequestURI().getQuery();
            int t = param(q, "t", 2), sec = param(q, "s", 30);
            long end = System.currentTimeMillis() + sec * 1000L;
            for (int i = 0; i < t; i++) {
                Thread th = new Thread(() -> {
                    double d = 0;
                    while (System.currentTimeMillis() < end)
                        d += Math.sqrt(d + 1);
                });
                th.setDaemon(true);
                th.start();
            }
            reply(x, "spinning " + t + " threads for " + sec + "s\n");
        });
        s.start();
        System.out.println("listening on :8080\n" + info());
    }

    static String info() {
        Runtime r = Runtime.getRuntime();
        return String.format(
            "availableProcessors=%d maxHeap=%dMiB "
                + "totalHeap=%dMiB used=%dMiB%n",
            r.availableProcessors(), r.maxMemory() >> 20,
            r.totalMemory() >> 20,
            (r.totalMemory() - r.freeMemory()) >> 20);
    }

    static int param(String q, String k, int dflt) {
        if (q == null) return dflt;
        for (String p : q.split("&")) {
            String[] kv = p.split("=", 2);
            if (kv.length == 2 && kv[0].equals(k))
                return Integer.parseInt(kv[1]);
        }
        return dflt;
    }

    static void reply(com.sun.net.httpserver.HttpExchange x, String body)
            throws java.io.IOException {
        byte[] b = body.getBytes();
        x.sendResponseHeaders(200, b.length);
        try (OutputStream o = x.getResponseBody()) { o.write(b); }
    }
}
