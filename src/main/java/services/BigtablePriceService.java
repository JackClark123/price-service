package services;



import com.beanstalk.core.bigtable.BeanstalkData;
import com.beanstalk.core.bigtable.entities.Identifier;
import com.beanstalk.core.bigtable.entities.Price;
import com.beanstalk.core.spanner.entities.account.PublicAccount;
import com.beanstalk.core.spanner.entities.group.BetGroup;
import com.beanstalk.core.spanner.entities.group.id.GroupMemberId;
import com.beanstalk.core.spanner.repositories.BetGroupMemberRepository;
import com.beanstalk.core.values.Project;
import com.beanstalk.core.values.Table;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.micronaut.http.HttpResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Singleton
public class BigtablePriceService implements PriceService{

    @Inject
    BetGroupMemberRepository betGroupMemberRepository;

    private BigtableDataClient dataClient;

    BigtablePriceService() throws IOException {
        dataClient = BigtableDataClient.create(Project.PROJECT, Table.INSTANCE);
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
    public HttpResponse<Price> price(UUID accountID, Identifier identifier) {

        if (identifier.getGroup() != null) {
            // check if member of group
            GroupMemberId groupMemberId = GroupMemberId.builder()
                    .betGroup(BetGroup.builder().id(identifier.getGroup()).build())
                    .publicAccount(PublicAccount.builder().id(accountID).build())
                    .build();

            if (!betGroupMemberRepository.existsById(groupMemberId)) {
                return HttpResponse.notFound();
            }
        }

        Price price = BeanstalkData.parse(dataClient.readRow(Table.PRICE, identifier.keyBuilder()), Price.class);

        if (price == null) {
            return HttpResponse.notFound();
        }

        return HttpResponse.ok(price);
    }

    @Override
    public HttpResponse<List<Price>> minute(UUID accountID, Identifier identifier) {

        if (identifier.getGroup() != null) {
            // check if member of group
            GroupMemberId groupMemberId = GroupMemberId.builder()
                    .betGroup(BetGroup.builder().id(identifier.getGroup()).build())
                    .publicAccount(PublicAccount.builder().id(accountID).build())
                    .build();

            if (!betGroupMemberRepository.existsById(groupMemberId)) {
                return HttpResponse.notFound();
            }
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_10M));
    }

    @Override
    public HttpResponse<List<Price>> hour(UUID accountID, Identifier identifier) {

        if (identifier.getGroup() != null) {
            // check if member of group
            GroupMemberId groupMemberId = GroupMemberId.builder()
                    .betGroup(BetGroup.builder().id(identifier.getGroup()).build())
                    .publicAccount(PublicAccount.builder().id(accountID).build())
                    .build();

            if (!betGroupMemberRepository.existsById(groupMemberId)) {
                return HttpResponse.notFound();
            }
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_1H));
    }

    @Override
    public HttpResponse<List<Price>> day(UUID accountID, Identifier identifier) {

        if (identifier.getGroup() != null) {
            // check if member of group
            GroupMemberId groupMemberId = GroupMemberId.builder()
                    .betGroup(BetGroup.builder().id(identifier.getGroup()).build())
                    .publicAccount(PublicAccount.builder().id(accountID).build())
                    .build();

            if (!betGroupMemberRepository.existsById(groupMemberId)) {
                return HttpResponse.notFound();
            }
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_1D));
    }

    @Override
    public HttpResponse<List<Price>> week(UUID accountID, Identifier identifier) {

        if (identifier.getGroup() != null) {
            // check if member of group
            GroupMemberId groupMemberId = GroupMemberId.builder()
                    .betGroup(BetGroup.builder().id(identifier.getGroup()).build())
                    .publicAccount(PublicAccount.builder().id(accountID).build())
                    .build();

            if (!betGroupMemberRepository.existsById(groupMemberId)) {
                return HttpResponse.notFound();
            }
        }

        Filters.Filter priceFilter = Filters.FILTERS.key().regex("^" + identifier.keyBuilder() + "#[0-9]+$");
        return HttpResponse.ok(prices(priceFilter, Table.PRICE_1W));
    }
}
