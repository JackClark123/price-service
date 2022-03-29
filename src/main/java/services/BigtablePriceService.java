package services;

import beanstalk.bigtable.Beanstalk;
import beanstalk.data.BeanstalkData;
import beanstalk.data.types.Identifier;
import beanstalk.data.types.Price;
import beanstalk.values.Project;
import beanstalk.values.Table;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.micronaut.http.HttpResponse;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class BigtablePriceService implements PriceService{

    private BigtableDataClient dataClient;

    BigtablePriceService() {
        // Creates the settings to configure a bigtable data client.
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(Project.PROJECT).setInstanceId(Table.INSTANCE).build();

        // Creates a bigtable data client.
        try {
            dataClient = BigtableDataClient.create(settings);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Price> prices(Filters.Filter priceFilter, String table) {
        List<Price> priceList = new ArrayList<>();

        // price at 10 minutes
        Query priceQuery = Query.create(table).filter(priceFilter).limit(100);
        ServerStream<Row> priceRows = dataClient.readRows(priceQuery);

        for (Row priceRow : priceRows) {
            priceList.add(BeanstalkData.parse(priceRow, Price.class));
        }

        return priceList;
    }

    @Override
    public HttpResponse<List<Price>> minute(String accountID, Identifier identifier) {

        if (identifier.getGroup() != null && !Beanstalk.isMember(dataClient, accountID, identifier.getGroup())) {
            return HttpResponse.notFound();
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_10M));
    }

    @Override
    public HttpResponse<List<Price>> hour(String accountID, Identifier identifier) {

        if (identifier.getGroup() != null && !Beanstalk.isMember(dataClient, accountID, identifier.getGroup())) {
            return HttpResponse.notFound();
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_1H));
    }

    @Override
    public HttpResponse<List<Price>> day(String accountID, Identifier identifier) {

        if (identifier.getGroup() != null && !Beanstalk.isMember(dataClient, accountID, identifier.getGroup())) {
            return HttpResponse.notFound();
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_1D));
    }

    @Override
    public HttpResponse<List<Price>> week(String accountID, Identifier identifier) {

        if (identifier.getGroup() != null && !Beanstalk.isMember(dataClient, accountID, identifier.getGroup())) {
            return HttpResponse.notFound();
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_1W));
    }
}
