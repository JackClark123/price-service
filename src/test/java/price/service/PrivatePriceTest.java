package price.service;

import beanstalk.data.types.*;
import beanstalk.values.GatewayHeader;
import beanstalk.values.Project;
import beanstalk.values.Table;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
public class PrivatePriceTest {

    @Inject
    @Client("/")
    HttpClient client;

    private BigtableDataClient dataClient;

    private static final String testAccountOne = "PrivateTestOne";
    private static final String testAccountTwo = "PrivateTestTwo";

    private static final long group = 1234567789L;

    PrivatePriceTest() {
        // Creates the settings to configure a bigtable data client.
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(Project.PROJECT).setInstanceId(Table.INSTANCE).build();

        // Creates a bigtable data client.
        try {
            dataClient = BigtableDataClient.create(settings);

            // Creates a bigtable table admin client.
        } catch (IOException e) {
            e.printStackTrace();
        }

        Account account = new Account();
        account.setFirstName("Test");
        account.setLastName("Account");
        account.setEmail(testAccountOne + "@test.com");
        account.setAccountID(testAccountOne);

        GroupMember groupMember = new GroupMember();
        groupMember.setGroupId(group);
        groupMember.setAccountId(testAccountOne);

        try {
            dataClient.mutateRow(account.toMutation(Table.ACCOUNT, account.getAccountID()));

            dataClient.mutateRow(groupMember.toMutation(Table.GROUP_MEMBER, group + "#" + testAccountOne));

        } catch (NotFoundException e) {
            System.err.println("Failed to write to non-existent table: " + e.getMessage());
        }

    }

    @Test
    void GetMinutePrices() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_10M, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/minute", identifier).header(GatewayHeader.account, testAccountOne);

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }

    @Test
    void GetHourPrices() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1H, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/hour", identifier).header(GatewayHeader.account, testAccountOne);

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }

    @Test
    void GetDayPrices() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1D, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/day", identifier).header(GatewayHeader.account, testAccountOne);

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }

    @Test
    void GetWeekPrices() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1W, identifier.keyBuilder() + "#" + timeKey));
            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/week", identifier).header(GatewayHeader.account, testAccountOne);

        HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);

        Assertions.assertEquals(100, rsp.body().size());

    }

    @Test
    void GetMinutePricesNotMember() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_10M, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/minute", identifier).header(GatewayHeader.account, testAccountTwo);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
            HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);
        });

        Assertions.assertEquals(HttpStatus.NOT_FOUND, e.getStatus());

    }

    @Test
    void GetHourPricesNotMember() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1H, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/hour", identifier).header(GatewayHeader.account, testAccountTwo);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
            HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);
        });

        Assertions.assertEquals(HttpStatus.NOT_FOUND, e.getStatus());

    }

    @Test
    void GetDayPricesNotMmeber() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1D, identifier.keyBuilder() + "#" + timeKey));

            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/day", identifier).header(GatewayHeader.account, testAccountTwo);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
            HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);
        });

        Assertions.assertEquals(HttpStatus.NOT_FOUND, e.getStatus());

    }

    @Test
    void GetWeekPricesNotMember() {
        Identifier identifier = new Identifier();
        identifier.setMarket(123456789L);
        identifier.setCompetitor(1L);
        identifier.setGroup(group);

        for (int i = 0; i < 200; i++) {

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long timeKey = Long.MAX_VALUE - System.nanoTime();

            Price price = new Price();
            price.setIdentifier(identifier);
            price.setMatched(i);
            price.setPrice(i);
            price.setBack(i);
            price.setLay(i);
            price.setProcessTime(Instant.now());

            // save to big table
            try {
                dataClient.mutateRow(price.toMutation(Table.PRICE_1W, identifier.keyBuilder() + "#" + timeKey));
            } catch (NotFoundException e) {
                System.err.println("Failed to write to non-existent table: " + e.getMessage());
            }
        }

        HttpRequest<?> request = HttpRequest.POST("/week", identifier).header(GatewayHeader.account, testAccountTwo);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
            HttpResponse<List<Price>> rsp = client.toBlocking().exchange(request, (Class<List<Price>>)(Object)List.class);
        });

        Assertions.assertEquals(HttpStatus.NOT_FOUND, e.getStatus());

    }

}
