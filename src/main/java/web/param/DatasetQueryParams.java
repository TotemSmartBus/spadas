package web.param;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import web.consts.QueryMode;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatasetQueryParams {
    int k = 10; //top-k
    int dim = 2; //dimension
    double[][] querydata;
    int datasetId = -1;
    //    0: Haus, 1: IA, 2: GBO, 3: EMD
    QueryMode mode = QueryMode.IA;
    //    int mode = 0;
    double error = 0.0;
    boolean approxi = true;
    boolean useIndex = true;
}
