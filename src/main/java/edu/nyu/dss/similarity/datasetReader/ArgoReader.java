package edu.nyu.dss.similarity.datasetReader;

import edu.nyu.dss.similarity.CityNode;
import edu.nyu.dss.similarity.config.SpadasConfig;
import edu.nyu.dss.similarity.statistics.DatasetSizeCounter;
import edu.nyu.dss.similarity.statistics.PointCounter;
import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import edu.whu.index.FilePathIndex;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import edu.nyu.dss.similarity.index.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ArgoReader {
    @Autowired
    private SpadasConfig config;

    @Autowired
    private DatasetSizeCounter datasetSizeCounter;

    @Autowired
    private PointCounter pointCounter;

    @Autowired
    private DatasetPerDir datasetPerDir;

    @Autowired
    private DatasetIDMapping datasetIDMapping;

    @Autowired
    private FileIDMap fileIDMap;

    @Autowired
    private DataMapPorto dataMapPorto;

    @Autowired
    private IndexBuilder indexBuilder;

    @Autowired
    private FilePathIndex filePathIndex;

    public Map<Integer, double[][]> read(File file, int fileNo, CityNode cityNode) {
        int i = 0;
        List<double[]> list = new ArrayList<>();
        double[][] data;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String strLine;
//			LineNumberReader lnr = new LineNumberReader(new FileReader(file));
//			lineNumber = lnr.getLineNumber() + 1;
//			a = new double[(int) lineNumber-1][];
            while ((strLine = br.readLine()) != null) {
                String[] splitString = strLine.split(",");
                String aString = splitString[0];
                if (aString.equals("TIMESTAMP")) {
                    continue;
                }
                double[] b = new double[config.getDimension()];
                b[0] = Double.parseDouble(splitString[6]);
                b[1] = Double.parseDouble(splitString[7]);
                if (b[1] < -85 || b[1] > -75) {
                    continue;
                }
                list.add(b);
                i++;
//					System.out.println(splitString[31]);
//                if (!splitString[12].equals("") && !splitString[11].equals("")) {
////                    try {
////                        b[0] = Double.parseDouble(splitString[12]);
////                        b[1] = Double.parseDouble(splitString[11]);
////                    } catch (Exception e) {
////                        System.out.println(e);
////                        System.out.println(file.getName());
////                        System.out.println(splitString[12]);
////                        System.out.println(splitString[11]);
////                        continue;
////                    }
////                    if (b[0] < 10 || b[1] < 10) {
////                        continue;
////                    }
//                    list.add(b);
//                    i++;
//                }
            }
            data = list.toArray(new double[i][]);
//            System.out.println("File " + file.getName() + " has " + list.size() + " lines");
            datasetIDMapping.put(fileNo, file.getName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (i > 0) {
            String shortName = FilenameUtils.removeExtension(file.getName());
            datasetSizeCounter.put(i);
            if (config.isCacheDataset()) {
                dataMapPorto.put(fileNo, data);
            }
            if (config.isCacheIndex()) {
//				createDatasetIndex(fileNo, xxx,1);
                IndexNode node = indexBuilder.createDatasetIndex(fileNo, data, 1, cityNode);
//                对数据进行基于网格的取样，减小数据量
                indexBuilder.samplingDataByGrid(data, fileNo, node);
                node.setFileName(shortName);
            }
        }
        return dataMapPorto;
    }
}
