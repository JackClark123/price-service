package services;

import com.beanstalk.core.bigtable.entities.Identifier;
import com.beanstalk.core.bigtable.entities.Price;
import com.beanstalk.core.values.GatewayHeader;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;

import java.util.List;
import java.util.UUID;

public interface PriceService {

    HttpResponse<Price> price(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier);

    HttpResponse<List<Price>> minute(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier);

    HttpResponse<List<Price>> hour(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier);

    HttpResponse<List<Price>> day(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier);

    HttpResponse<List<Price>> week(@Header(GatewayHeader.account) UUID accountID, @Body Identifier identifier);

}
