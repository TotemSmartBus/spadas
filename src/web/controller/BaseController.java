package web.controller;

import au.edu.rmit.mtree.tests.Data;
import au.edu.rmit.trajectory.clustering.kmeans.indexNode;
import com.github.xiaoymin.knife4j.annotations.DynamicParameter;
import com.github.xiaoymin.knife4j.annotations.DynamicResponseParameters;
import edu.nyu.dss.similarity.Framework;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import web.DTO.PreviewDTO;
import web.DTO.dsqueryDTO;
import web.DTO.keywordsDTO;
import web.DTO.rangequeryDTO;
import web.Utils.FileU;
import web.Utils.FileUtil;
import web.VO.CityVo;
import web.VO.DatasetVo;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * @author Tian Qi Qing
 * @version 1.0
 **/

@Api
@RestController
@CrossOrigin
public class BaseController {
//    @Autowired
//    private FileUtil fileService;

//    @ApiOperation("load dataset")
//    @DynamicResponseParameters(name = "LoadDataHashMapModel", properties = {
//            @DynamicParameter(name = "nodes", value = "dataset node", dataTypeClass = indexNode.class),
//            @DynamicParameter(name = "filenames", value = "dataset's id-filename map"),
//    })
    @DynamicResponseParameters(name = "cityModel", properties = {
            @DynamicParameter(name = "cityNodeList", value = "city node list"),
            @DynamicParameter(name = "indexNodes", value = "index nodes"),
            @DynamicParameter(name = "datasetIdMapping", value = "dataset id mapping")
    })
    @ApiImplicitParam(name = "id", value = "no.id data")
    @RequestMapping(value = "spadas/api/load", method = RequestMethod.GET)
    public CityVo loadData(@RequestParam int id) throws IOException, InterruptedException {
        System.out.println("load data!");
//        Framework.init();
        System.out.println("stop here!");
        int len = Framework.cityNodeList.size();
        int batch = 10;
        int end = Math.min(batch * (id + 1), len);
        CityVo vo = new CityVo(Framework.cityNodeList.subList(batch * id, end));
//        TimeUnit.SECONDS.sleep(2);
        return vo;
    }
//    @ApiOperation("load dataset")
//    @DynamicResponseParameters(name = "LoadDataHashMapModel", properties = {
//            @DynamicParameter(name = "data", value = "data point"),
//            @DynamicParameter(name = "filenames", value = "dataset's id-filename map"),
//    })
//    @RequestMapping(value = "/loaddata", method = RequestMethod.GET)
//    public HashMap<String, Object> loadData() throws IOException {
//        System.out.println("load data!");
//        Framework.init();
//        System.out.println("stop here!");
//        HashMap a = new HashMap() {{
//            put("data", Framework.dataPoint);
//            put("filenames", Framework.datasetIdMapping);
//        }};
////        return new HashMap(){{put("nodes", Framework.indexNodes); put("filenames",Framework.datasetIdMapping); }};
//        return a;
//    }

//    @ApiOperation("test")
//    @DynamicResponseParameters(name = "testModel", properties = {
//            @DynamicParameter(name = "points", value = "data point")
//    })
//    @RequestMapping(value = "/test", method = RequestMethod.GET)
//    public double[][] test() {
//        System.out.println("test");
//        System.out.println(Framework.dataMapPorto.size());
//        return Framework.dataMapPorto.get(6);
//    }

    @ApiOperation("get dataset by id")
    @RequestMapping(value = "/spadas/api/getds", method = RequestMethod.GET)
    @ApiImplicitParam(name = "id", value = "dataset id")
    @DynamicResponseParameters(name = "SingleDatasetDataVO", properties = {
            @DynamicParameter(name = "node", value = "node summary", dataTypeClass = DatasetVo.class),
    })
    public Map<String, Object> getDatasetById(@RequestParam int id) {
        System.out.println(id);
        return new HashMap() {{
            put("node", new DatasetVo() {{
                        setNode(Framework.indexNodes.get(id));
                        setId(id);
                        setMatrix(Framework.dataMapPorto.get(id));
                        setFilename(Framework.datasetIdMapping.get(id));
                    }}
            );
        }};
    }

//    @ApiOperation("test")
//    @DynamicResponseParameters(name = "testModel", properties = {
//            @DynamicParameter(name = "list", value = "list of double")
//    })
////    @ApiImplicitParam(name = "length", value = "length of list")
//    @RequestMapping(value = "/test", method = RequestMethod.POST)
//    public List<Double> test(@RequestBody String lengthStr) {
//        System.out.println(lengthStr);
//
//        int realLength = 10000 * 100;
//        List<Double> res = new ArrayList<>();
//        Random rd = new Random();
//        while (realLength > 0) {
//            res.add(rd.nextDouble() * 100);
//            realLength--;
//        }
//        return res;
//    }

    @ApiOperation("range query")
    @DynamicResponseParameters(name = "RangeQueryModel", properties = {
            @DynamicParameter(name = "nodes", value = "list of indexNode", dataTypeClass = indexNode.class),
    })
    @RequestMapping(value = "spadas/api/rangequery", method = RequestMethod.POST)
    public Map<String, Object> rangequery(@RequestBody rangequeryDTO qo) {
        HashMap<String, Object> result = new HashMap();
        result.put("nodes", Framework.rangequery(qo));
        if (qo.getCityName() != "") {
            result.put("subCityNodes", Framework.cityIndexNodeMap.get(qo.getCityName()));
        }
        return result;
    }

    @ApiOperation("keywords query")
    @DynamicResponseParameters(name = "KeywordsQueryModel", properties = {
            @DynamicParameter(name = "nodes", value = "list of indexNode", dataTypeClass = indexNode.class),
    })
    @RequestMapping(value = "spadas/api/keywordsquery", method = RequestMethod.POST)
    public Map<String, Object> keywordsquery(@RequestBody keywordsDTO qo) {
        return new HashMap() {{
            put("nodes", Framework.keywordsQuery(qo));
        }};
    }


    @ApiOperation("upload dataset")
    @DynamicResponseParameters(name = "UploadModel", properties = {
            @DynamicParameter(name = "matrix", value = "2d array of uploaded dataset matrix", dataTypeClass = indexNode.class),
    })
    @RequestMapping(value = "spadas/api/uploaddataset", method = RequestMethod.POST)
//    public Map<String,Object> uploadDataset(@RequestParam("file") MultipartFile file) throws IOException {
//        String filename = fileService.uploadFile(file);
////        argo: 4
////        poi: 5
//        Framework.datalakeID=4;
////        call readSingleFile method
//        double [][] matrix = Framework.readSingleFile("/argoverse/"+filename);
//        return new HashMap(){{put("matrix",matrix);}};
//    }
    public void uploadDataset(@RequestParam("file") MultipartFile[] files) throws IOException {
//        comment for safety
//        for (MultipartFile file : files) {
//            String fileName = fileService.uploadFile(file);
//        }


//        String fileName = fileService.uploadFile(file);
//        File[] files = fileFolder.listFiles();
//        Framework.readFolder(fileFolder, 270000);
//        String filename = fileService.uploadFile(file);
//        argo: 4
//        poi: 5
//        Framework.datalakeID=4;
//        call readSingleFile method
//        double [][] matrix = Framework.readSingleFile("/argoverse/"+filename);
//        return new HashMap(){{put("matrix",matrix);}};
    }

    @ApiOperation("query by dataset")
    @DynamicResponseParameters(name = "DatasetSearchModel", properties = {
            @DynamicParameter(name = "nodes", value = "list of DatasetVO", dataTypeClass = DatasetVo.class),
    })
    @RequestMapping(value = "spadas/api/dsquery", method = RequestMethod.POST)
    public Map<String, Object> datasetQuery(@RequestBody dsqueryDTO qo) throws IOException {
        List<DatasetVo> result = Framework.datasetQuery(qo);
        return new HashMap() {{
            put("nodes", result);
        }};
    }

    @ApiOperation("download dataset")
    @GetMapping("spadas/api/file/{filename:.+}")
    public void downloadFile(@PathVariable String filename, HttpServletRequest request, HttpServletResponse response) {
//        fileService.downloadFile(filename, request, response);
    }

    @ApiOperation("data preview after join or union")
    @DynamicResponseParameters(name = "PreviewModel", properties = {
            @DynamicParameter(name = "headers", value = "headers of csv, Type: List<String[]>"),
            @DynamicParameter(name = "bodies", value = "bodies of csv, Type: List<String[][]>"),
    })
    @PostMapping("spadas/api/preview")
    public Map<String, Object> datasetPreview(@RequestBody PreviewDTO dto) {
        List<String[]> headers = new ArrayList<>();
        List<String[][]> bodies = new ArrayList<>();
        dto.getIds().forEach(id -> {
            try {
                Pair<String[], String[][]> pair = FileU.readPreviewDataset(Framework.datasetIdMapping.get(id), dto.getRows(), Framework.dataMapPorto.get(id));
                headers.add(pair.getLeft());
                bodies.add(pair.getRight());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return new HashMap() {{
            put("headers", headers);
            put("bodies", bodies);
        }};
    }


    @ApiOperation("dataset join")
    @RequestMapping(value = "spadas/api/join", method = RequestMethod.GET)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "queryId", value = "query id"),
            @ApiImplicitParam(name = "datasetId", value = "dataset id"),
            @ApiImplicitParam(name = "rows", value = "preview rows")
    })
    @DynamicResponseParameters(name = "JoinModel", properties = {
            @DynamicParameter(name = "header", value = "header of join data, Type: String[]"),
            @DynamicParameter(name = "joinData", value = "body of join data, Type: List<String[]>"),
    })
//    @GetMapping("/join")
    public Map<String, Object> datasetJoin(@RequestParam int queryId, @RequestParam int datasetId, @RequestParam int rows) throws IOException {
        Pair<Double[], Map<Integer, Integer>> pair = Framework.pairwiseJoin(queryId, datasetId);
        double[][] queryData = Framework.dataMapPorto.get(queryId);
        double[][] datasetData = Framework.dataMapPorto.get(datasetId);
        Pair<String[], String[][]> querydata = FileU.readPreviewDataset(Framework.datasetIdMapping.get(queryId), rows, queryData);
        Pair<String[], String[][]> basedata = FileU.readPreviewDataset(Framework.datasetIdMapping.get(datasetId), Integer.MAX_VALUE, datasetData);

        //int len = querydata.getRight()[0].length+basedata.getRight()[0].length;
        String[] joinHeaderTemp = ArrayUtils.addAll(querydata.getLeft(), basedata.getLeft());
        String[] distHeader = {"distance(km)"};
        String[] joinHeader = ArrayUtils.addAll(joinHeaderTemp, distHeader);
//        List<String[]> joindata = pair.getRight().entrySet().stream()
//                .map(idPair -> ArrayUtils.addAll(querydata.getRight()[idPair.getValue()], basedata.getRight()[idPair.getValue()])).collect(Collectors.toList());
        List<String[]> joinData = new ArrayList<>();
        String[] queryEntry;
        String[] baseEntry;
        Double[] distEntry = pair.getLeft();
        String[] distEntryTemp;
        for (int i = 0; i < querydata.getRight().length; i++) {
            queryEntry = querydata.getRight()[i];
            baseEntry = basedata.getRight()[pair.getRight().get(i + 1) - 1];
            double tmp = Math.round(distEntry[i] * 1000) / 1000.000;
            String tmpStr = tmp < 5 ? String.valueOf(tmp) : "INVALID";
            distEntryTemp = new String[]{tmpStr};
            joinData.add(ArrayUtils.addAll(ArrayUtils.addAll(queryEntry, baseEntry), distEntryTemp));
        }
        System.out.println();
        return new HashMap() {{
            put("header", joinHeader);
            put("joinData", joinData);
        }};
    }

    @ApiOperation("union range search")
    @DynamicResponseParameters(name = "UnionModel", properties = {
            @DynamicParameter(name = "header", value = "header of join data, Type: String[]"),
            @DynamicParameter(name = "joinData", value = "body of join data, Type: List<String[]>"),
    })
    @RequestMapping(value = "spadas/api/union", method = RequestMethod.POST)
    public Map<String, Object> datasetUnion(@RequestBody dsqueryDTO qo) throws IOException {
        Framework.UnionRangeQuery(qo.getQuerydata(), qo.getDatasetId(), 2);
        return new HashMap() {{
            put("nodes", Framework.indexNodes.get(1));
        }};
    }


//    @RequestMapping(value = "/test",method = RequestMethod.GET)
//    public Map<String,Object> test1() throws IOException {
//        return new HashMap(){{put("nodes",Framework.indexNodes.get(1));}};
//
//    }


}
