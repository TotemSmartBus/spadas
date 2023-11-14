package web.service;

import lombok.Data;
import org.springframework.stereotype.Component;
import web.consts.TrajectoryDistanceType;
import web.consts.TrajectoryIndexType;

@Component
@Data
public class TrajectoryAugmentOptions {
    private TrajectoryIndexType indexType;

    private TrajectoryDistanceType distanceType;

    public TrajectoryAugmentOptions() {
        indexType = TrajectoryIndexType.NONE;
        distanceType = TrajectoryDistanceType.POINT;
    }
}
