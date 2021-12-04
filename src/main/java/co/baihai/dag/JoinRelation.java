package co.baihai.dag;

import java.util.List;

/**
 * Copyright BaiHai.ai (c) all right reserved.
 * Project: halo-pipeline
 * Author   : lz@baihai.ai
 * Created: 2021-12-04 09:58
 */
public enum JoinRelation {
    ALL_SUCCESS {
        @Override
        public boolean satisfy(List<Status> preStatus) {
            return preStatus.stream().allMatch(Status::isSuccessful);
        }
    },
    ALL_FINISH {
        @Override
        public boolean satisfy(List<Status> preStatus) {
            return preStatus.stream().allMatch(Status::isFinished);
        }
    },
    ONE_SUCCESS {
        @Override
        public boolean satisfy(List<Status> preStatus) {
            return preStatus.stream().anyMatch(Status::isSuccessful);
        }
    },
    ONE_FINISH {
        @Override
        public boolean satisfy(List<Status> preStatus) {
            return preStatus.stream().anyMatch(Status::isFinished);
        }
    };

    public boolean satisfy(List<Status> preStatus) {
        return false;
    }
}
