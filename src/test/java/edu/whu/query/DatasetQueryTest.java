package edu.whu.query;

import edu.nyu.dss.similarity.index.DatasetIDMapping;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import web.SpadasWebApplication;
import web.VO.DatasetVo;
import web.consts.QueryMode;
import web.param.DatasetQueryParams;
import web.service.FrameworkService;

import java.io.IOException;
import java.util.List;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpadasWebApplication.class)
public class DatasetQueryTest {
    @Autowired
    private FrameworkService frameworkService;

    @Autowired
    private DatasetIDMapping datasetIdMapping;

    @Test
    public void generalDatasetQueryTest() throws IOException, CloneNotSupportedException {
        int queryId = 4000;
        int k = 30;

        DatasetQueryParams params = new DatasetQueryParams();
        params.setDatasetId(queryId);
        params.setK(k);
        
        String datasetName = datasetIdMapping.get(queryId);
        log.info("current dataset name is {}, id is {}", datasetName, queryId);

        for (QueryMode queryMode : QueryMode.values()) {
            params.setMode(queryMode);
            query(params);
        }

//        params.setMode(QueryMode.Haus);
//        query(params);
//        params.setMode(QueryMode.IA);
//        query(params);
//        params.setMode(QueryMode.GBO);
//        query(params);
//        params.setMode(QueryMode.EMD);
//        query(params);
        System.out.println();
    }

    private void query(DatasetQueryParams params) throws IOException, CloneNotSupportedException {
        List<DatasetVo> vos = frameworkService.datasetQuery(params);
        log.info("result of {} : ", params.getMode());
        printDatasetVo(vos);
    }

    private void printDatasetVo(List<DatasetVo> vos) {
        for (DatasetVo vo : vos) {
            System.out.println(vo.getId() + " " + vo.getFilename());
        }
    }
}
