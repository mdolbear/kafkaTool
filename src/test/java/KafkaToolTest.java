import com.mjdsoftware.kafkatool.KafkaToolApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import static junit.framework.TestCase.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = KafkaToolApplication.class,
                webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class KafkaToolTest {


    @LocalServerPort
    private int port;

    //Constants
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "clientId";
    private static final String GROUP_ID = "mygroup";
    private static final String STRING_KEY_CLASS_SHORTNAME = "String";
    private static final String STRING_VALUE_CLASS_SHORTNAME = "String";
    private static final String TOPIC_NAME = "game-events";


    /**
     * Subscribe and remove subscription test
     */
    @Test
    public void subscribeAndRemoveSubscriptionTest() throws Exception {


        String                  tempBaseUrl;
        String                  tempSubscriptionName = "mysub";

        //Create base url
        System.out.println("Random port generated for test: " + this.getPort());
        tempBaseUrl = "http://localhost:" + this.getPort() +"/kafkatool";

        //Test subscription
        this.performTopicSubscribe(tempBaseUrl,
                                   tempSubscriptionName,
                                   BOOTSTRAP_SERVERS,
                                   CLIENT_ID,
                                   GROUP_ID,
                                   STRING_KEY_CLASS_SHORTNAME,
                                   STRING_VALUE_CLASS_SHORTNAME,
                                   TOPIC_NAME);


    }


    /**
     * Answer my port for testing
     * @return
     */
    private int getPort() {
        return port;
    }

    /**
     * Subscribe to a topic using the following parameters:
     * @param aBaseUrl String
     * @param aSubscriptionName String
     * @param aBootstrapServers String
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClassShortName String
     * @param aDeserializerValueClassShortName String
     * @param aTopic String
     * @throws Exception
     */
    protected void performTopicSubscribe(String aBaseUrl,
                                         String aSubscriptionName,
                                         String aBootstrapServers,
                                         String aClientId,
                                         String aGroupId,
                                         String aDeserializerKeyClassShortName,
                                         String aDeserializerValueClassShortName,
                                         String aTopic) throws Exception {


        RestTemplate                                tempTemplate;
        ResponseEntity<String>                      tempEntity;
        HttpEntity<MultiValueMap<String, String>> tempRequest;

        tempTemplate = new RestTemplate();
        tempRequest = this.createArgumentsForTopicSubscribe(aSubscriptionName,
                                                            aBootstrapServers,
                                                            aClientId,
                                                            aGroupId,
                                                            aDeserializerKeyClassShortName,
                                                            aDeserializerValueClassShortName,
                                                            aTopic);
        tempEntity =
                tempTemplate.postForEntity(aBaseUrl + "/subscribe",
                                           tempRequest,
                                           String.class);
        assertTrue("Status code failure",
                tempEntity.getStatusCode().equals(HttpStatus.OK));

    }


    /**
     * Create parameters for topic subscribe
     * @param aSubscriptionName String
     * @param aBootstrapServers Strinbg
     * @param aClientId String
     * @param aGroupId String
     * @param aDeserializerKeyClassShortName String
     * @param aDeserializerValueClassShortName String
     * @param aTopic String
     * @return HttpEntity
     */
    private HttpEntity<MultiValueMap<String, String>>
                            createArgumentsForTopicSubscribe(String aSubscriptionName,
                                                             String aBootstrapServers,
                                                             String aClientId,
                                                             String aGroupId,
                                                             String aDeserializerKeyClassShortName,
                                                             String aDeserializerValueClassShortName,
                                                             String aTopic) {

        HttpHeaders                                     tempHeaders;
        MultiValueMap<String, String>                   tempParams;
        HttpEntity<MultiValueMap<String, String>>       tempRequest;

        tempHeaders = new HttpHeaders();
        tempHeaders.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        tempParams = new LinkedMultiValueMap<String, String>();
        tempParams.add("name", aSubscriptionName);
        tempParams.add("bootstrapServers", aBootstrapServers);
        tempParams.add("clientId", aClientId);
        tempParams.add("groupId", aGroupId);
        tempParams.add("desKeyClassShortName", aDeserializerKeyClassShortName);
        tempParams.add("desValueClassShortName", aDeserializerValueClassShortName);
        tempParams.add("topic", aTopic);
        tempRequest = new HttpEntity<MultiValueMap<String, String>>(tempParams, tempHeaders);

        return tempRequest;

    }

}