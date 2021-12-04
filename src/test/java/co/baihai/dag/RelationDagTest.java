package co.baihai.dag;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * Copyright BaiHai.ai (c) all right reserved.
 * Project: wizard_dag
 * Author   : lz@baihai.ai
 * Created: 2021-12-04 20:30
 */
public class RelationDagTest {
    static class DemoTask implements DagNode {
        private final String name;
        private Status status;

        public DemoTask(String name){
            this.name = name;
            this.status = Status.READY;
        }

        @Override
        public void run() {
            this.status = Status.RUNNING;
            System.out.println(this.name + " finished");
            this.status = Status.SUCCESS;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public Status status() {
            return this.status;
        }
    }
    DemoTask a = new DemoTask("a");
    DemoTask b = new DemoTask("b");
    DemoTask c = new DemoTask("c");
    DemoTask d = new DemoTask("d");
    DemoTask e = new DemoTask("e");
    DemoTask f = new DemoTask("f");
    DemoTask g = new DemoTask("g");
    DemoTask h = new DemoTask("h");
    DemoTask j = new DemoTask("j");

    @Test
    void testCircle () {
        RelationDag dag1 = new RelationDag(2, 2, 1, true);
        dag1.addEdge(a, a);
        assert dag1.hasCircle();

        RelationDag dag2 = new RelationDag(2, 2, 1, true);
        dag2.addEdge(a, b);
        dag2.addEdge(b, c);
        dag2.addEdge(c, a);
        assert dag2.hasCircle();

        RelationDag dag3 = new RelationDag(2, 2, 1, true);
        dag3.addEdge(a, b);
        dag3.addEdge(b, c);
        dag3.addEdge(c, d);
        assert ! dag3.hasCircle();

        RelationDag dag4 = new RelationDag(2, 2, 1, true);
        dag4.addEdge(a, b);
        dag4.addEdge(b, c);
        dag4.addEdge(c, b);
        assert dag4.hasCircle();

        RelationDag dag5 = new RelationDag(2, 2, 1, true);
        dag5.addEdge(a, b);
        dag5.addEdge(b, c);
        dag5.addEdge(c, b);
        dag5.addEdge(c, e);
        assert dag5.hasCircle();

        RelationDag dag6 = new RelationDag(2, 2, 1, true);
        dag5.addEdge(a, b);
        dag5.addEdge(c, b);
        dag5.addEdge(d, b);
        assert ! dag6.hasCircle();

        RelationDag dag = new RelationDag(10, 2, 1, true);
        dag.addEdge(h, g);
        dag.addEdge(g, b);
        dag.addEdge(a, b);
        dag.addEdge(b, f);
        dag.addEdge(c, d);
        dag.addEdge(d, e);
        dag.addEdge(e, f);
        dag.addEdge(f, j);
        assert ! dag.hasCircle();
    }

    @Test
    void testAllSuccess() {
        RelationDag dag = new RelationDag(10, 2, 1, true);
        dag.addEdge(h, g);
        dag.addEdge(g, b);
        dag.addEdge(a, b);
        dag.addEdge(b, f);
        dag.addEdge(c, d);
        dag.addEdge(d, e);
        dag.addEdge(e, f);
        dag.addEdge(f, j);

//        DAG chain = dag.chain();
        /**
         *  H
         *    \
         *      G
         *        \
         *     A -> B
         *            \
         *  C- D  -E  - F-> J
         */
        dag.run();

        HashMap<String, Integer> order = dag.finishedTaskOrders;

        assert(order.size() == 9);
        assert(order.get("a") < order.get("b"));
        assert(order.get("h") < order.get("g"));
        assert(order.get("g") < order.get("b"));
        assert(order.get("c") < order.get("d"));
        assert(order.get("d") < order.get("e"));
        assert(order.get("e") < order.get("f"));
        assert(order.get("f") < order.get("j"));
    }
}