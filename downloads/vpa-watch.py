#!/usr/bin/env python3
"""VPA watcher: polls the VPA, the pod, namespace events and the JVM's own
view every POLL seconds, keeps the last few hours in memory, and serves a
live page plus a JSON API. Stdlib only.

  python3 vpa-watch.py             # in-cluster, via the ServiceAccount
  python3 vpa-watch.py --demo      # fabricated data, for a local preview
"""
import json, os, re, ssl, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NS = os.environ.get("NAMESPACE") or open(
    "/var/run/secrets/kubernetes.io/serviceaccount/namespace").read().strip() \
    if not "--demo" in sys.argv else "demo"
VPA = os.environ.get("VPA_NAME", "load")
APP_LABEL = os.environ.get("APP_LABEL", "app=load")
CONTAINER = os.environ.get("CONTAINER", "load")
JVM_URL = os.environ.get("JVM_URL", "http://load:8080/")
POLL = int(os.environ.get("POLL_SECONDS", "10"))
KEEP = int(os.environ.get("KEEP_SAMPLES", "1080"))  # 3h at 10s
PORT = int(os.environ.get("PORT", "8080"))
DEMO = "--demo" in sys.argv

STATE = {"samples": [], "events": {}, "meta": {}, "error": None, "note": None}
LOCK = threading.Lock()

# ---------------------------------------------------------------- k8s access
def k8s_client():
    base = "https://%s:%s" % (os.environ["KUBERNETES_SERVICE_HOST"],
                              os.environ["KUBERNETES_SERVICE_PORT"])
    tok = open("/var/run/secrets/kubernetes.io/serviceaccount/token").read()
    ctx = ssl.create_default_context(
        cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
    def get(path):
        req = urllib.request.Request(base + path,
                                     headers={"Authorization": "Bearer " + tok})
        with urllib.request.urlopen(req, context=ctx, timeout=8) as r:
            return json.load(r)
    return get

def qty(s):
    """Kubernetes quantity -> float (cores for cpu, bytes for memory)."""
    if s is None: return None
    s = str(s)
    m = re.match(r"^([0-9.]+)([a-zA-Z]*)$", s)
    if not m: return None
    n, u = float(m.group(1)), m.group(2)
    mult = {"": 1, "m": 1e-3, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
            "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}
    return n * mult.get(u, 1)

def jvm_view():
    try:
        with urllib.request.urlopen(JVM_URL, timeout=3) as r:
            txt = r.read().decode()
        d = dict(re.findall(r"(\w+)=(\d+)", txt))
        return {"procs": int(d.get("availableProcessors", 0)),
                "maxHeap": int(d.get("maxHeap", 0)) * 2**20,
                "usedHeap": int(d.get("used", 0)) * 2**20}
    except Exception:
        return None

def poll_once(get):
    now = time.time()
    vpa = get("/apis/autoscaling.k8s.io/v1/namespaces/%s/verticalpodautoscalers/%s"
              % (NS, VPA))
    pods = get("/api/v1/namespaces/%s/pods?labelSelector=%s" % (NS, APP_LABEL))
    evs = get("/api/v1/namespaces/%s/events?limit=500" % NS)

    rec = {}
    for c in (vpa.get("status", {}).get("recommendation", {})
              .get("containerRecommendations", [])):
        if c.get("containerName") == CONTAINER:
            rec = c
    conds = {c["type"]: c["status"] for c in vpa.get("status", {}).get("conditions", [])}

    # newest running pod
    live = [p for p in pods.get("items", []) if not p["metadata"].get("deletionTimestamp")]
    live.sort(key=lambda p: p["metadata"]["creationTimestamp"])
    pod = live[-1] if live else None
    res, restarts, resize, podname, rpol = {}, 0, "", None, {}
    if pod:
        podname = pod["metadata"]["name"]
        for c in pod["spec"]["containers"]:
            if c["name"] == CONTAINER:
                res = c.get("resources", {})
                rpol = {r["resourceName"]: r.get("restartPolicy", "NotRequired")
                        for r in c.get("resizePolicy", [])}
        for cs in pod.get("status", {}).get("containerStatuses", []):
            if cs["name"] == CONTAINER: restarts = cs.get("restartCount", 0)
        resize = pod.get("status", {}).get("resize", "")

    spec = vpa.get("spec", {})
    pol = {}
    for c in spec.get("resourcePolicy", {}).get("containerPolicies", []):
        if c.get("containerName") in (CONTAINER, "*"):
            pol = {"minCpu": qty(c.get("minAllowed", {}).get("cpu")),
                   "maxCpu": qty(c.get("maxAllowed", {}).get("cpu")),
                   "minMem": qty(c.get("minAllowed", {}).get("memory")),
                   "maxMem": qty(c.get("maxAllowed", {}).get("memory"))}
    sample = {
        "t": now, "pod": podname, "restarts": restarts, "resize": resize,
        "mode": spec.get("updatePolicy", {}).get("updateMode"),
        "resizePolicy": rpol, "policy": pol,
        "recProvided": conds.get("RecommendationProvided") == "True",
        "cpu": {"req": qty(res.get("requests", {}).get("cpu")),
                "lim": qty(res.get("limits", {}).get("cpu")),
                "target": qty(rec.get("target", {}).get("cpu")),
                "lower": qty(rec.get("lowerBound", {}).get("cpu")),
                "upper": qty(rec.get("upperBound", {}).get("cpu")),
                "uncapped": qty(rec.get("uncappedTarget", {}).get("cpu"))},
        "mem": {"req": qty(res.get("requests", {}).get("memory")),
                "lim": qty(res.get("limits", {}).get("memory")),
                "target": qty(rec.get("target", {}).get("memory")),
                "lower": qty(rec.get("lowerBound", {}).get("memory")),
                "upper": qty(rec.get("upperBound", {}).get("memory")),
                "uncapped": qty(rec.get("uncappedTarget", {}).get("memory"))},
        "jvm": jvm_view(),
    }
    meta = {"namespace": NS, "vpa": VPA, "container": CONTAINER,
            "updateMode": vpa.get("spec", {}).get("updatePolicy", {}).get("updateMode"),
            "conditions": conds, "poll": POLL}

    target = spec.get("targetRef", {}).get("name")
    names = {VPA, target} | {p["metadata"]["name"] for p in pods.get("items", [])}
    new_events = {}
    for e in evs.get("items", []):
        obj = e.get("involvedObject", {})
        if obj.get("name") not in names:
            continue
        key = e["metadata"]["uid"] + ":" + str(e.get("count", 1))
        ts = e.get("lastTimestamp") or e.get("eventTime") or e["metadata"]["creationTimestamp"]
        new_events[key] = {"t": iso_to_epoch(ts), "kind": obj.get("kind"),
                           "name": obj.get("name"), "reason": e.get("reason"),
                           "type": e.get("type"), "message": (e.get("message") or "")[:300],
                           "source": (e.get("source", {}) or {}).get("component")
                                     or e.get("reportingComponent")}
    # Instructor notes: `oc annotate vpa load vpa-watch/note="..."`. Each new
    # value becomes a marker, stamped when the watcher first sees it.
    note = vpa.get("metadata", {}).get("annotations", {}).get("vpa-watch/note")
    with LOCK:
        if note and note != STATE["note"]:
            new_events["note:%d" % int(now)] = {
                "t": now, "kind": "Note", "name": VPA, "reason": "Note",
                "type": "Normal", "message": note, "source": "instructor"}
        STATE["note"] = note
        STATE["samples"].append(sample)
        del STATE["samples"][:-KEEP]
        STATE["events"].update(new_events)
        STATE["meta"] = meta
        STATE["error"] = None

def iso_to_epoch(s):
    try:
        return time.mktime(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone
    except Exception:
        return time.time()

def poller():
    get = None if DEMO else k8s_client()
    if DEMO:  # backfill the scenario so the page has history at once
        t = DEMO_T0
        while t < time.time(): demo_step(t); t += POLL
    while True:
        try:
            if DEMO: demo_step()
            else: poll_once(get)
        except Exception as ex:  # keep serving stale data, report the error
            with LOCK: STATE["error"] = "%s: %s" % (type(ex).__name__, ex)
        time.sleep(POLL)

# ---------------------------------------------------------------- demo data
DEMO_T0 = time.time() - 20 * 60
def demo_step(now=None):
    """Replays the dry run: Off-mode recs climb, then an in-place resize,
    then an OOM restart, then a RestartContainer memory resize."""
    now = now or time.time(); el = now - DEMO_T0
    Mi = 2**20; Gi = 2**30
    cpu_lim, mem_lim, restarts, procs, heap = 0.5, 512*Mi, 0, 1, 371*Mi
    tgt_cpu = min(2.0, 0.1 + el / 120); tgt_mem = min(548*Mi, 256*Mi + el * 300e3)
    if el > 480:  cpu_lim, mem_lim, procs = 2.935, 1047*Mi, 3
    if el > 720:  restarts, heap = 1, 759*Mi
    if el > 1020: mem_lim, restarts, heap = 1536*Mi, 2, 1152*Mi
    s = {"t": now, "pod": "load-66d8dd45cb-snk2b", "restarts": restarts,
         "resize": "", "mode": "Off" if el < 300 else "InPlaceOrRecreate",
         "recProvided": el > 5,
         "resizePolicy": {"cpu": "NotRequired",
                          "memory": "NotRequired" if el < 900 else "RestartContainer"},
         "policy": {"minCpu": 0.1, "maxCpu": 2.0, "minMem": 256*Mi, "maxMem": 2*Gi},
         "cpu": {"req": cpu_lim/5 if el <= 480 else 2.0, "lim": cpu_lim, "target": tgt_cpu,
                 "lower": tgt_cpu*0.6, "upper": min(2.0, tgt_cpu*1.6), "uncapped": tgt_cpu*1.5},
         "mem": {"req": mem_lim/2 if el <= 480 else 548*Mi, "lim": mem_lim, "target": tgt_mem,
                 "lower": tgt_mem*0.85, "upper": tgt_mem*1.3, "uncapped": tgt_mem},
         "jvm": {"procs": procs, "maxHeap": heap,
                 "usedHeap": min(heap*0.85, 20*Mi + el*250e3) if el < 720 else 160*Mi}}
    evs = {}
    def ev(k, t, kind, name, reason, typ, msg, src):
        if el >= t: evs[k] = {"t": DEMO_T0 + t, "kind": kind, "name": name, "reason": reason,
                              "type": typ, "message": msg, "source": src}
    ev("a", 5, "VerticalPodAutoscaler", "load", "RecommendationProvided", "Normal",
       "First recommendation written", "vpa-recommender")
    ev("b", 480, "Pod", "load-66d8dd45cb-snk2b", "InPlaceResizedByVPA", "Normal",
       "Resized container load: cpu 500m->2935m, memory 512Mi->1047Mi", "vpa-updater")
    ev("b2", 481, "Pod", "load-66d8dd45cb-snk2b", "ResizeCompleted", "Normal",
       "Pod resize completed: {\"load\":{\"limits\":{\"cpu\":\"2935m\",\"memory\":\"1047Mi\"},\"requests\":{\"cpu\":\"587m\",\"memory\":\"523Mi\"}}}", "kubelet")
    ev("n1", 300, "Note", "load", "Note", "Normal",
       "Module 4, Ex 1: switched the VPA to InPlaceOrRecreate", "instructor")
    ev("n2", 700, "Note", "load", "Note", "Normal",
       "Ex 3: retained 300 MiB, now asking for 100 more", "instructor")
    ev("c", 720, "Pod", "load-66d8dd45cb-snk2b", "BackOff", "Warning",
       "Container load exited 137 (OutOfMemoryError, ExitOnOutOfMemoryError)", "kubelet")
    ev("d", 722, "Pod", "load-66d8dd45cb-snk2b", "Started", "Normal",
       "Started container load", "kubelet")
    ev("e", 1020, "Pod", "load-66d8dd45cb-snk2b", "Killing", "Normal",
       "Container load definition changed, will be restarted (memory resize, RestartContainer)", "kubelet")
    ev("f", 1023, "Pod", "load-66d8dd45cb-snk2b", "Started", "Normal",
       "Started container load", "kubelet")
    with LOCK:
        STATE["samples"].append(s); del STATE["samples"][:-KEEP]
        STATE["events"].update(evs)
        STATE["meta"] = {"namespace": "demo", "vpa": "load", "container": "load",
                         "updateMode": "Off" if el < 300 else "InPlaceOrRecreate",
                         "conditions": {"RecommendationProvided": "True"}, "poll": POLL}

# ---------------------------------------------------------------- http
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        if self.path.startswith("/api/state"):
            with LOCK:
                body = json.dumps({"meta": STATE["meta"], "error": STATE["error"],
                                   "samples": STATE["samples"],
                                   "events": sorted(STATE["events"].values(),
                                                    key=lambda e: e["t"])})
            return self.send(200, body.encode(), "application/json")
        if self.path.startswith("/healthz"):
            return self.send(200, b"ok", "text/plain")
        return self.send(200, PAGE.encode(), "text/html; charset=utf-8")

PAGE = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "vpa-watch.html")).read() if os.path.exists(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "vpa-watch.html")) else "<h1>missing vpa-watch.html</h1>"

if __name__ == "__main__":
    threading.Thread(target=poller, daemon=True).start()
    print("vpa-watch on :%d (%s)" % (PORT, "demo" if DEMO else NS), flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
