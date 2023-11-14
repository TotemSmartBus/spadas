package web.VO;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class JoinVO {
    private List<String> header;
    @JsonProperty("joinData")
    private List<List<String>> body;
}
