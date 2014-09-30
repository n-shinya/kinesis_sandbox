package samples.elasticsearch;

import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import samples.KinesisMessageModel;

import java.util.Arrays;
import java.util.List;

public class SuspectUserFilter implements IFilter<KinesisMessageModel> {

    private static List<String> suspectedUser = Arrays.asList(
        "iq_mon45",
        "newswktk"
    );

    @Override
    public boolean keepRecord(KinesisMessageModel record) {
        String retweetedUser = record.getRetweetedUser();
        if (retweetedUser == null) {
            return true;
        }
        if (suspectedUser.contains(retweetedUser)) {
            return false;
        }
        return true;
    }
}
