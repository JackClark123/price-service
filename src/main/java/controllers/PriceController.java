package controllers;

import com.beanstalk.core.bigtable.entities.Identifier;
import com.beanstalk.core.bigtable.entities.Price;
import com.beanstalk.core.bigtable.repositories.CreateIfNotExists;
import com.beanstalk.core.values.GatewayHeader;
import com.beanstalk.core.values.Project;
import com.beanstalk.core.values.Table;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import services.PriceService;

import java.util.List;
import java.util.UUID;

@ExecuteOn(TaskExecutors.IO)
@Controller("/")
public class PriceController {

    @Inject
    PriceService priceService;

    @EventListener
    public void onStartupEvent(StartupEvent event) {

        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE, Price.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_10M, Price.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_1H, Price.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_1D, Price.class);
        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.PRICE_1W, Price.class);

    }

    @Post(value = "/", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<Price> price(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier) {
        return priceService.price(accountID, identifier);
    }

    @Post(value = "/minute", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> minute(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier) {
        return priceService.minute(accountID, identifier);
    }

    @Post(value = "/hour", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> hour(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier) {
        return priceService.hour(accountID, identifier);
    }

    @Post(value = "/day", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> day(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier) {
        return priceService.day(accountID, identifier);
    }

    @Post(value = "/week", processes = MediaType.APPLICATION_JSON)
    public HttpResponse<List<Price>> week(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier) {
        return priceService.week(accountID, identifier);
    }

}
