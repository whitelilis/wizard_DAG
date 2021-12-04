package co.baihai.dag;

/**
 * Copyright BaiHai.ai (c) all right reserved.
 * Project: halo-pipeline
 * Author   : lz@baihai.ai
 * Created: 2021-12-04 09:57
 */
public interface DagNode extends Runnable {
    default JoinRelation joinRelation(){
        return JoinRelation.ALL_FINISH;
    }
    default int retryTimes(){
        return 0;
    }
    String name();
    Status status();
}
