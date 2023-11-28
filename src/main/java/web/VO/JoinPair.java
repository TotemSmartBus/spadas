package web.VO;

import lombok.Data;

@Data
public class JoinPair {

    private JoinPoint queryPoint;

    private JoinPoint targetPoint;

    private double distance;
}
