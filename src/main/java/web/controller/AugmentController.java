package web.controller;

import edu.whu.structure.DatasetID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import web.VO.DatasetVo;
import web.VO.JoinVO;
import web.VO.PreviewVO;
import web.VO.RoadMatchPair;
import web.param.AugmentParams;
import web.param.UnionParams;
import web.param.UnionRangeQueryParams;
import web.service.FrameworkService;
import web.service.TrajectoryAugmentService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
@RestController
public class AugmentController {

    @Autowired
    private FrameworkService framework;

    @Autowired
    private TrajectoryAugmentService augmentService;

    @GetMapping("/spadas/api/augment")
    public DatasetVo getDatasetAugment(@RequestBody AugmentParams params) {
        log.info("you are querying augment for dataset {}", params.getDatasetID());
        return framework.datasetAugment(params.getDatasetID(), params.getDatasetCount(), params.getCandidateCount(), params.getOptions());
    }


    @RequestMapping(value = "spadas/api/join", method = RequestMethod.GET)
    public JoinVO datasetJoin(@RequestParam int queryId, @RequestParam int datasetId, @RequestParam int rows) throws IOException {
        return framework.join(queryId, datasetId, rows);
    }

    @PostMapping("spadas/api/union")
    public PreviewVO datasetUnion(@RequestBody UnionParams dto) {
        String type = "union";
        List<String> headers = Arrays.asList("lat", "lng");
        List<List<double[]>> body = framework.union(dto);
        return new PreviewVO(type, headers, body);
    }

    @PostMapping("spadas/api/unionRangeQuery")
    public PreviewVO datasetUnionRangeQuery(@RequestBody UnionRangeQueryParams dto) {
        String type = "union";
        List<String> headers = Arrays.asList("lat", "lng");
        List<List<double[]>> body = framework.unionRangeQuery(dto);
        return new PreviewVO(type, headers, body);
    }


    @PostMapping("spadas/api/findRoad")
    public List<RoadMatchPair> findRoad(@RequestParam int id) {
        return augmentService.findNearestRoad(new DatasetID(id));
    }

}
