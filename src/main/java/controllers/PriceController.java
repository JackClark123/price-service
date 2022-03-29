package controllers;

import beanstalk.bigtable.CreateIfNotExists;
import beanstalk.data.types.*;
import beanstalk.values.GatewayHeader;
import beanstalk.values.Project;
import beanstalk.values.Table;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Inject;
import services.PriceService;

import java.util.List;

@Controller("/")
public class PriceController {

    @Inject
    PriceService priceService;

    @EventListener
    public void onStartupEvent(StartupEvent event) {

        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.ACCOUNT, Account.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.GROUP, Group.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.GROUP_MEMBER, GroupMember.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_10M, Price.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_1H, Price.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_1D, Price.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_1W, Price.class);

    }

    @Post(value = "/minute", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> minute(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier) {
        return priceService.minute(accountID, identifier);
    }

    @Post(value = "/hour", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> hour(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier) {
        return priceService.hour(accountID, identifier);
    }

    @Post(value = "/day", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> day(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier) {
        return priceService.day(accountID, identifier);
    }

    @Post(value = "/week", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> week(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier) {
        return priceService.week(accountID, identifier);
    }

}
