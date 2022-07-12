package price.service;


import com.beanstalk.core.bigtable.entities.Identifier;
import com.beanstalk.core.bigtable.entities.Price;
import com.beanstalk.core.spanner.entities.account.PublicAccount;
import com.beanstalk.core.spanner.entities.group.BetGroup;
import com.beanstalk.core.spanner.entities.group.BetGroupMember;
import com.beanstalk.core.spanner.repositories.AccountRepository;
import com.beanstalk.core.spanner.repositories.BetGroupMemberRepository;
import com.beanstalk.core.spanner.repositories.BetGroupRepository;
import com.beanstalk.core.values.GatewayHeader;
import com.beanstalk.core.values.Project;
import com.beanstalk.core.values.Table;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import helper.RandomID;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@MicronautTest
public class PublicPriceTest {

    @Inject
    @Client("/")
    HttpClient client;

    private BigtableDataClient dataClient;

    final AccountRepository accountRepository;

    final BetGroupRepository betGroupRepository;

    final BetGroupMemberRepository betGroupMemberRepository;

    PublicAccount testAccountOne;

    PublicAccount testAccountTwo;

    PublicPriceTest(AccountRepository accountRepository, BetGroupRepository betGroupRepository,  BetGroupMemberRepository betGroupMemberRepository) throws IOException {
        this.accountRepository = accountRepository;
        this.betGroupRepository = betGroupRepository;
        this.betGroupMemberRepository = betGroupMemberRepository;

        dataClient = BigtableDataClient.create(Project.PROJECT, Table.INSTANCE);

        PublicAccount publicAccount = PublicAccount.builder()
                .firstName("Test")
                .lastName("Account")
                .email(RandomID.generate() + "@test.com")
                .build();

        PublicAccount publicAccountTwo = PublicAccount.builder()
                .firstName("Test")
                .lastName("Account")
                .email(RandomID.generate() + "@test.com")
                .build();

        testAccountOne = this.accountRepository.save(publicAccount);
        testAccountTwo = this.accountRepository.save(publicAccountTwo);

    }

    @Test
    void GetMinutePrices() {
        Identifier identifier = Identifier.builder()
                .market(UUID.randomUUID())
                .competitor(UUID.randomUUID())
                .build();

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = Price.builder()
                    .identifier(identifier)
                    .matched(i)
                    .price(i)
                    .back(i)
                    .lay(i)
                    .processTime(Timestamp.from(java.time.Instant.now()))
                    .build();

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_10M, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/minute", identifier).header(GatewayHeader.account, testAccountOne.getId().toString());

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }

    @Test
    void GetHourPrices() {
        Identifier identifier = Identifier.builder()
                .market(UUID.randomUUID())
                .competitor(UUID.randomUUID())
                .build();

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = Price.builder()
                    .identifier(identifier)
                    .matched(i)
                    .price(i)
                    .back(i)
                    .lay(i)
                    .processTime(Timestamp.from(java.time.Instant.now()))
                    .build();

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1H, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/hour", identifier).header(GatewayHeader.account, testAccountOne.getId().toString());

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }

    @Test
    void GetDayPrices() {
        Identifier identifier = Identifier.builder()
                .market(UUID.randomUUID())
                .competitor(UUID.randomUUID())
                .build();

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = Price.builder()
                    .identifier(identifier)
                    .matched(i)
                    .price(i)
                    .back(i)
                    .lay(i)
                    .processTime(Timestamp.from(java.time.Instant.now()))
                    .build();

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1D, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/day", identifier).header(GatewayHeader.account, testAccountOne.getId().toString());

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }

    @Test
    void GetWeekPrices() {
        Identifier identifier = Identifier.builder()
                .market(UUID.randomUUID())
                .competitor(UUID.randomUUID())
                .build();

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = Price.builder()
                    .identifier(identifier)
                    .matched(i)
                    .price(i)
                    .back(i)
                    .lay(i)
                    .processTime(Timestamp.from(Instant.now()))
                    .build();

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1W, identifier.keyBuilder() + "#" + timeKey));
            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/week", identifier).header(GatewayHeader.account, testAccountOne.getId().toString());

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }
}
