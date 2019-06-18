package com.kineticdata.bridgehub.adapter.aveksa;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.net.URL;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import org.apache.http.conn.ssl.SSLSocketFactory;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.LoggerFactory;

public class AveksaAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/

    /** Defines the adapter display name */
    public static final String NAME = "Aveksa Bridge";

    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(AveksaAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(AveksaAdapter.class.getResourceAsStream("/"+AveksaAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+AveksaAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String PROPERTY_USERNAME = "Username";
        public static final String PROPERTY_PASSWORD = "Password";
        public static final String PROPERTY_URL = "Aveksa Url";
    }

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.PROPERTY_USERNAME).setIsRequired(true),
        new ConfigurableProperty(Properties.PROPERTY_PASSWORD).setIsRequired(true).setIsSensitive(true),
        new ConfigurableProperty(Properties.PROPERTY_URL).setIsRequired(true)
    );

    private String username;
    private String password;
    private URL url;
    private String token;

    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public void initialize() throws BridgeError {
        this.username = properties.getValue(Properties.PROPERTY_USERNAME);
        this.password = properties.getValue(Properties.PROPERTY_PASSWORD);
        try {
            this.url = new URL(properties.getValue(Properties.PROPERTY_URL));
        } catch (MalformedURLException e) {
            logger.error("Error Output: " + e.getMessage());
            throw new BridgeError("Invalid URL: '" + properties.getValue(Properties.PROPERTY_URL) + "' is a malformed URL.", e);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }

    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        AveksaQualificationParser parser = new AveksaQualificationParser();
        String filterParameters = parser.parse(request.getQuery(),request.getParameters());

        String command = "find" + request.getStructure();

        StringBuilder getUrl = new StringBuilder();
        getUrl.append(this.url.toString());
        getUrl.append("/aveksa/command.submit?cmd=");
        getUrl.append(command);
        getUrl.append("&format=json");
        if (!filterParameters.equals("*")) {
            getUrl.append("&").append(filterParameters);
        }
        // Possibly add sorting and possibly pagination data later on

//        // Getting a different httpClient that will work with all SSL Certificates (for use in dev environments)
//        HttpClient client;
//        try {
//            client = getTestingHttpClient();
//        } catch (Exception e) {
//            throw new BridgeError(e);
//        }
        HttpClient client = HttpClients.createDefault();
        System.out.println(getUrl.toString() + "&" + this.token);
        logger.debug(getUrl.toString() + "&" + this.token);
        HttpGet get = new HttpGet(getUrl.toString() + "&" + this.token);

        String output = "";
        HttpResponse response;
        try {
            response = client.execute(get);
            // If the token is expired, try refreshing and doing the call again before throwing an exception
            if (response.getStatusLine().getStatusCode() == 401) {
                EntityUtils.consume(response.getEntity());
                this.token = authenticate(this.url.toString(),this.username,this.password);
                HttpGet secondGet = new HttpGet(getUrl.toString() + "&" + this.token);
                response = client.execute(secondGet);
            }
            output = EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            throw new BridgeError(e);
        }

        // If the response threw a 404 error with the command name as the reason,,
        // throw and notify the user of the bad structure.
        if (response.getStatusLine().getStatusCode() == 404) {
            if (response.getStatusLine().getReasonPhrase().equals(command)) {
                throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure because '" + command + "' is not a valid Aveksa information command.");
            }
        }

        JSONObject jsonObj = (JSONObject)JSONValue.parse(output);
        JSONArray structureList = (JSONArray)jsonObj.get(command);

        //Return the response
        return new Count(Long.valueOf(structureList.size()));
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        AveksaQualificationParser parser = new AveksaQualificationParser();
        String filterParameters = parser.parse(request.getQuery(),request.getParameters());
        List<String> fields = request.getFields();

        String returnColumns = request.getFieldString();
        String command = "find" + request.getStructure();

        StringBuilder getUrl = new StringBuilder();
        getUrl.append(this.url.toString());
        getUrl.append("/aveksa/command.submit?cmd=");
        getUrl.append(command);
        getUrl.append("&format=json");
        getUrl.append("&returnColumns=").append(returnColumns);
        if (!filterParameters.equals("*")) {
            getUrl.append("&").append(filterParameters);
        }
        // Possibly add sorting and possibly pagination data later on
        getUrl.append("&").append(this.token);

//        //Getting a different httpClient that will work with all SSL Certificates (for use in dev environments)
//        HttpClient client;
//        try {
//            client = getTestingHttpClient();
//        } catch (Exception e) {
//            throw new BridgeError(e);
//        }
        HttpClient client = HttpClients.createDefault();
        HttpGet get = new HttpGet(getUrl.toString());

        String output = "";
        HttpResponse response;
        try {
            response = client.execute(get);
            // If the token is expired, try refreshing and doing the call again before throwing an exception
            if (response.getStatusLine().getStatusCode() == 401) {
                EntityUtils.consume(response.getEntity());
                this.token = authenticate(this.url.toString(),this.username,this.password);
                HttpGet secondGet = new HttpGet(getUrl.toString() + "&" + this.token);
                response = client.execute(secondGet);
            }
            output = EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            throw new BridgeError(e);
        }

        // If the response threw a 404 error with the command name as the reason,,
        // throw and notify the user of the bad structure.
        if (response.getStatusLine().getStatusCode() == 404) {
            if (response.getStatusLine().getReasonPhrase().equals(command)) {
                throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure because '" + command + "' is not a valid Aveksa information command.");
            }
        }

        // Check to see if the response code message contains an 'Executing JDBC
        // query failed'  to throw a specifc error for bad query, general unexpected error
        // for anything else that falls under the 500 error code
        if (response.getStatusLine().getStatusCode() != 200) {
            if (response.getStatusLine().getReasonPhrase().contains("Executing JDBC query failed")) {
                logger.error("Error Reason: " + response.getStatusLine().getReasonPhrase());
                throw new BridgeError("Invalid Query: The query string '" + filterParameters + "' appears to include invalid elements.");
            } else if (response.getStatusLine().getStatusCode() == 412) {
                throw new BridgeError(response.getStatusLine().getReasonPhrase());
            }  else {
                logger.error("Error Reason: " + response.getStatusLine().getReasonPhrase());
                throw new BridgeError("An unexpected error was encountered.");
            }
        }

        Record record = new Record(null);
        JSONObject jsonOutput = (JSONObject)JSONValue.parse(output);

        if (jsonOutput.size() > 1) {
            throw new BridgeError("Multiple results matched an expected single match query");
        } else if (jsonOutput.isEmpty()) {
            record = new Record(null);
        } else {
            for (Object o : jsonOutput.keySet()) {
                String key = o.toString();
                if (!key.equals("Date") && !key.equals("Status") && !key.equals("ErrorCode") && !key.equals("Total")) {
                    JSONObject deviceJson = (JSONObject)JSONValue.parse(jsonOutput.get(key).toString());
                    Map<String,Object> recordMap = new LinkedHashMap<String,Object>();
                    if (fields == null) { fields = new ArrayList( deviceJson.entrySet()); }
                    for (String field : fields) {
                        recordMap.put(field, deviceJson.get(field));
                    }
                    record = new Record(recordMap);
                }
            }
        }

        // Returning the response
        return record;
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        AveksaQualificationParser parser = new AveksaQualificationParser();
        String filterParameters = parser.parse(request.getQuery(),request.getParameters());

        String returnColumns = request.getFieldString();
        String command = "find" + request.getStructure();

        StringBuilder getUrl = new StringBuilder();
        getUrl.append(this.url.toString());
        getUrl.append("/aveksa/command.submit?cmd=");
        getUrl.append(command);
        getUrl.append("&format=json");
        getUrl.append("&returnColumns=").append(returnColumns);
        if (!filterParameters.equals("*")) {
            getUrl.append("&").append(filterParameters);
        }
        // Possibly add sorting and possibly pagination data later on
        getUrl.append("&").append(this.token);

//        // Getting a different httpClient that will work with all SSL Certificates (for use in dev environments)
//        HttpClient client;
//        try {
//            client = getTestingHttpClient();
//        } catch (Exception e) {
//            throw new BridgeError(e);
//        }
        HttpClient client = HttpClients.createDefault();
        HttpGet get = new HttpGet(getUrl.toString());

        String output = "";
        HttpResponse response;
        try {
            response = client.execute(get);
            // If the token is expired, try refreshing and doing the call again before throwing an exception
            if (response.getStatusLine().getStatusCode() == 401) {
                EntityUtils.consume(response.getEntity());
                this.token = authenticate(this.url.toString(),this.username,this.password);
                HttpGet secondGet = new HttpGet(getUrl.toString() + "&" + this.token);
                response = client.execute(secondGet);
            }
            output = EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            throw new BridgeError(e);
        }

        // If the response threw a 404 error with the command name as the reason,,
        // throw and notify the user of the bad structure.
        if (response.getStatusLine().getStatusCode() == 404) {
            if (response.getStatusLine().getReasonPhrase().equals(command)) {
                throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure because '" + command + "' is not a valid Aveksa information command.");
            }
        }

        // Check to see if the response code message contains an 'Executing JDBC
        // query failed'  to throw a specifc error for bad query, general unexpected error
        // for anything else that falls under the 500 error code
        if (response.getStatusLine().getStatusCode() != 200) {
            if (response.getStatusLine().getReasonPhrase().contains("Executing JDBC query failed")) {
                logger.error("Error Reason: " + response.getStatusLine().getReasonPhrase());
                throw new BridgeError("Invalid Query: The query string '" + filterParameters + "' appears to include invalid elements.");
            } else if (response.getStatusLine().getStatusCode() == 412) {
                throw new BridgeError(response.getStatusLine().getReasonPhrase());
            }  else {
                logger.error("Error Reason: " + response.getStatusLine().getReasonPhrase());
                throw new BridgeError("An unexpected error was encountered.");
            }
        }

        JSONObject jsonObj = (JSONObject)JSONValue.parse(output);
        JSONArray structureList = (JSONArray)jsonObj.get(command);

        List<String> fields = request.getFields();
        ArrayList<Record> records = new ArrayList<Record>();
        for (int i = 0; i < structureList.size(); i++) {
            List record = new ArrayList();
            JSONObject structureObj = (JSONObject)JSONValue.parse(structureList.get(i).toString());
            for (String field : fields) {
                record.add(structureObj.get(field));
            }
            records.add((Record) record);
        }

        // Building the output metadata
        Map<String,String> metadata = BridgeUtils.normalizePaginationMetadata(request.getMetadata());
        metadata.put("pageSize", String.valueOf("0"));
        metadata.put("pageNumber", String.valueOf("1"));
        metadata.put("offset", String.valueOf("0"));
        metadata.put("size", String.valueOf(records.size()));
        metadata.put("count", metadata.get("size"));

        // Returning the response
        return new RecordList(fields, records, metadata);
    }

    /*----------------------------------------------------------------------------------------------
     * PRIVATE HELPER METHODS
     *--------------------------------------------------------------------------------------------*/

    /**
     * A helper method that authenticates the System Admin user
     * and returns a token in the form of token={token} which will be
     * appended to the other bridge calls for authentication
     */
    public String authenticate(String url, String username, String password) throws BridgeError {
        String token = "";
        String postUrl = url.toString() + "/aveksa/command.submit?cmd=loginUser";

        // Getting a different httpClient that will work with all SSL Certificates (for use in dev environments)
        // HttpClient client;
        // try {
        //     client = getTestingHttpClient();
        // } catch (Exception e) {
        //     throw new BridgeError(e);
        // }
        HttpClient client = HttpClients.createDefault();
        HttpPost post = new HttpPost(postUrl);

        String loginCreds = String.format("<username>%s</username><password>%s</password>",username,password);
        try {
            StringEntity entity = new StringEntity(loginCreds);
            post.setEntity(entity);
            post.setHeader("Content-Type","application/xml");
        } catch (UnsupportedEncodingException e) {
            throw new BridgeError("Unable to add body to HttpPost object",e);
        }

        try {
            HttpResponse response = client.execute(post);
            token = EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            throw new BridgeError(e);
        }

        // If the token was returned with a \n char, strip it from the token string
        token = token.replace("\n", "");

        logger.debug(token);

        return token;
    }

    // Both the getTrustingManger and getTestingHttpClient methods SHOULD NOT
    // BE USED IN A PRODUCTION ENVIRONMENT.
    private HttpClient getTestingHttpClient() throws NoSuchAlgorithmException, KeyManagementException {
        HttpClient httpclient = HttpClients.createDefault();

        X509HostnameVerifier hostnameVerifier = org.apache.http.conn.ssl.SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, getTrustingManager(), new java.security.SecureRandom());

        SSLSocketFactory socketFactory = new SSLSocketFactory(sc);
        socketFactory.setHostnameVerifier(hostnameVerifier);
        Scheme sch = new Scheme("https", 443, socketFactory);
        httpclient.getConnectionManager().getSchemeRegistry().register(sch);

        return httpclient;
    }

    private TrustManager[] getTrustingManager() {
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
                // Do nothing
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
                // Do nothing
            }

        } };
        return trustAllCerts;
    }

}
