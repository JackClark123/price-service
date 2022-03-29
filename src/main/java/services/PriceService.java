package services;

import beanstalk.data.types.Identifier;
import beanstalk.data.types.Price;
import beanstalk.values.GatewayHeader;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;

import java.util.List;

public interface PriceService {

    HttpResponse<List<Price>> minute(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier);

    HttpResponse<List<Price>> hour(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier);

    HttpResponse<List<Price>> day(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier);

    HttpResponse<List<Price>> week(@Header(GatewayHeader.account) String accountID, @Body Identifier identifier);

}
