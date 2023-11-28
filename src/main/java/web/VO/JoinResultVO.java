package web.VO;

import lombok.Data;

import java.util.List;

@Data
public class JoinResultVO {

    private int queryDatasetID;

    private int targetDatasetID;

    List<JoinPair> list;
}
