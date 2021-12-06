package co.baihai.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.Math.min;

/**
 * Copyright BaiHai.ai (c) all right reserved.
 * Project: halo-pipeline
 * Author   : lz@baihai.ai
 * Created: 2021-12-04 10:18
 */
public class RelationDag implements Runnable{
    public static Logger logger = LoggerFactory.getLogger(RelationDag.class);
    public final int taskParallel;
    public final int retryTimes;
    public Status status;
    // using node name as key
    public HashMap<String, DagNode> tasks;
    // for from and to, we only record node names;
    public HashMap<String, ArrayList<String>> nextTaskNames;
    public HashMap<String, ArrayList<String>> preTaskNames;

    public ConcurrentHashMap<String, Integer> runningTimes;
    public HashSet<String> toScheduleNames;

    public ExecutorService runningPool;
    public final long checkIntervalSeconds;
    public final boolean goodManMode;
    public HashMap<String, Integer> finishedTaskOrders;

    public RelationDag(int taskParallel, int retryTimes, int checkIntervalSeconds, boolean goodManMode) {
        this.taskParallel = taskParallel;
        this.retryTimes = retryTimes;
        this.checkIntervalSeconds = checkIntervalSeconds;
        this.goodManMode = goodManMode;
        tasks = new HashMap<>();
        nextTaskNames = new HashMap<>();
        preTaskNames = new HashMap<>();
        runningTimes = new ConcurrentHashMap<>();
        toScheduleNames = new HashSet<>();
        this.runningPool = Executors.newFixedThreadPool(taskParallel);
        this.finishedTaskOrders = new HashMap<>();
    }

    // todo: handle hung node, with neither pre nor post
    public void addEdge(DagNode from, DagNode to){
        if (from != null) {
            tasks.put(from.name(), from);
            toScheduleNames.add(from.name());
        }
        if (to != null) {
            tasks.put(to.name(), to);
            toScheduleNames.add(to.name());
        }

        if (from != null && to != null) {
            ArrayList<String> nexts = nextTaskNames.getOrDefault(from.name(), new ArrayList<>());
            nexts.add(to.name());
            nextTaskNames.put(from.name(), nexts);

            ArrayList<String> pres = preTaskNames.getOrDefault(to.name(), new ArrayList<>());
            pres.add(from.name());
            preTaskNames.put(to.name(), pres);
        }
    }

    // https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    static class NodeWrapper {
        public String inner;
        public int index;
        public int lowLink;
        public boolean onStack = false;
        public NodeWrapper(String inner, int index) {
            this.inner = inner;
            this.index = index;
        }
    }

    static class Vars {
        public int index = 0;
        LinkedList<NodeWrapper> stack = new LinkedList<>();
    }

    public boolean hasCircle(){
        Vars var = new Vars();
        HashMap<String, NodeWrapper> wrapped = new HashMap<>();
        for (String v: tasks.keySet()) {
            wrapped.put(v, new NodeWrapper(v, -1));
        }

        for (NodeWrapper v : wrapped.values()) {
            if (v.index < 0) {
                if (strongConnect(wrapped, v, var)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean strongConnect(HashMap<String, NodeWrapper> wrapped, NodeWrapper v, Vars var) {
        v.index = var.index;
        v.lowLink = var.index;
        var.index = var.index + 1;
        var.stack.push(v);
        v.onStack = true;

        for (String wString : this.nextTaskNames.getOrDefault(v.inner, new ArrayList<>())) {
            NodeWrapper w = wrapped.get(wString);
            if (w.index < 0) {
                if (strongConnect(wrapped, w, var) ) {
                    return true;
                }
                v.lowLink = min(v.lowLink, w.lowLink);
            } else if (w.onStack) {
                v.lowLink = min(v.lowLink, w.index);
            }
        }

        if (v.lowLink == v.index) {
            HashSet<String> part = new HashSet<>();
            while(true) {
                NodeWrapper w = var.stack.pop();
                w.onStack = false;
                part.add(w.inner);
                if (w.inner.equals(v.inner)) {
                    break;
                }
            }
            if (part.size() > 1) {
                return true;
            } else if (part.size() == 1) {
                // a -> a   circle
                for (String onlyOne: part) {
                    ArrayList<String> nexts = nextTaskNames.getOrDefault(onlyOne, new ArrayList<>());
                    if (nexts.stream().anyMatch(x -> x.equals(onlyOne))) {
                        return true;
                    }
                }
            }
        } else {
            return false;
        }
        return false;
    }

    public Status finalStatus() {
        return this.status;
    }

    public void schedule(DagNode task) {
        logger.info("scheduled {}", task.name());
        int alreadyRunTimes = runningTimes.getOrDefault(task.name(), 0);
        runningTimes.put(task.name(), alreadyRunTimes + 1);
        runningPool.submit(task);
    }

    public void finishOrReschedule(DagNode task) throws MaxRetryStillFailedExecption {
        if (task.status().isFailed()) {
            int usingRetryTimes = task.retryTimes() > 0 ? task.retryTimes() : this.retryTimes;
            if (runningTimes.getOrDefault(task.name(), 0) >= usingRetryTimes) {
                throw new MaxRetryStillFailedExecption(task.name());
            } else {// retry
                if (goodManMode) {
                    schedule(task);
                } else {
                    finishTask(task);
                }
            }
        } else if (task.status().isSuccessful()) {
            finishTask(task);
        } else {
            logger.info("{} is still running", task.name());
        }
    }

    private void finishTask(DagNode task) {
        runningTimes.remove(task.name());
        toScheduleNames.remove(task.name());
        finishedTaskOrders.put(task.name(), finishedTaskOrders.size());
    }

    /**
     * Any next task joinRelation is AllSuccessful, need all success.
     * @param task
     * @return
     */
    public boolean someNextNeedAllSuccess(DagNode task) {
        ArrayList<String> nextTaskNames = this.nextTaskNames.get(task.name());
        return nextTaskNames.stream()
                .map(name -> tasks.get(name).joinRelation())
                .anyMatch(r -> r == JoinRelation.ALL_SUCCESS);
    }

    /**
     * Some next task need AnySuccess, but all other pre failed,
     * so all dependent on this task
     * @param task
     * @return
     */
    private boolean someNextDependOnThisSuccess(DagNode task) {
        ArrayList<String> nextTaskNames = this.nextTaskNames.get(task.name());
        for (String taskName: nextTaskNames) {
            DagNode t = tasks.get(taskName);
            if (t.joinRelation() == JoinRelation.ONE_SUCCESS) {
                ArrayList<String> preTaskNames = this.preTaskNames.get(t.name());
                // if all pre: is the checkRunningOne or is failed/timeout, then that is it
                if (preTaskNames.stream()
                        .allMatch(name -> name.equals(task.name()) || tasks.get(name).status().isFailed())) {
                    return true;
                }
            }
        }
        return false;
    }

    public void run() {
        if (this.hasCircle()) {
            this.status = Status.EXCEPTION;
            logger.error("dag has circle");
            return;
        } else {
            while (runningTimes.size() > 0 || toScheduleNames.size() > 0) {
                // clean running tasks first
                for (String taskName : runningTimes.keySet()) {
                    DagNode t = tasks.get(taskName);
                    if (tasks.get(taskName).status().isFinished()) {
                        try {
                            finishOrReschedule(t);
                        } catch (MaxRetryStillFailedExecption e) {
                            if (someNextNeedAllSuccess(t) || someNextDependOnThisSuccess(t)) {
                                this.status = t.status();
                                return;
                            } else { // current failed or timeout can be ignore
                                finishTask(t);
                            }
                        }
                    }
                }
                // then check to schedule tasks
                for (String taskName : toScheduleNames) {
                    DagNode t = tasks.get(taskName);
                    ArrayList<String> preTaskNames = this.preTaskNames.get(taskName);
                    if (preTaskNames == null || preTaskNames.isEmpty()) { // if no pre node
                        schedule(t);
                    } else { // has pre node
                        List<Status> preStatus = preTaskNames.stream().map(name -> tasks.get(name).status()).collect(Collectors.toList());
                        if (t.joinRelation().satisfy(preStatus)) {
                            schedule(t);
                        }
                    }
                }
                // all to schedule checked, wait for another check circle
                try {
                    Thread.sleep(this.checkIntervalSeconds * 1000);
                } catch (InterruptedException e) {
                    logger.error("sleep error", e);
                }
            }
            try {
                this.runningPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("stop DAG run pool failed", e);
            }
            this.runningPool.shutdown();
            this.status = Status.SUCCESS;
            return;
        }
    }
}
