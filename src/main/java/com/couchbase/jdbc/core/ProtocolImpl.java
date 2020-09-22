/*
 * //  Copyright (c) 2015 Couchbase, Inc.
 * //  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * //  except in compliance with the License. You may obtain a copy of the License at
 * //    http://www.apache.org/licenses/LICENSE-2.0
 * //  Unless required by applicable law or agreed to in writing, software distributed under the
 * //  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * //  either express or implied. See the License for the specific language governing permissions
 * //  and limitations under the License.
 */

package com.couchbase.jdbc.core;


import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.jdbc.CBResultSet;
import com.couchbase.jdbc.CBStatement;
import com.couchbase.jdbc.ConnectionParameters;
import com.couchbase.jdbc.connect.Cluster;
import com.couchbase.jdbc.connect.Instance;
import com.couchbase.jdbc.connect.Protocol;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Created by davec on 2015-02-22.
 */
public class ProtocolImpl implements Protocol
{

    private static final String STATEMENT="statement";
    private static final String ENCODING = "encoding";
    private static final String NAMESPACE = "namespace";
    private static final String READ_ONLY = "readonly";
    private static final String TIMEOUT = "timeout";
    private static final String CREDENTIALS = "creds";
    private static final String SCAN_CONSITENCY = "scan_consistency";

    private static final int N1QL_ERROR = -1;
    private static final int N1QL_SUCCESS = 0;
    private static final int N1QL_RUNNING = 1;
    private static final int N1QL_COMPLETED = 2;
    private static final int N1QL_STOPPED = 3;
    private static final int N1QL_TIMEOUT = 4;
    private static final int N1QL_FATAL = 5;

    private Gson gson = new Gson();
    private Type listOfMap = new TypeToken<Map<String, List<Map<String, Object>>>>(){}.getType();
    private Type listOfCouchError = new TypeToken<List<CouchError>>(){}.getType();

    static final Map <String,Integer> statusStrings = new HashMap<String,Integer>();
    static {
        statusStrings.put( "errors", N1QL_ERROR );
        statusStrings.put( "success", N1QL_SUCCESS );
        statusStrings.put( "running", N1QL_RUNNING );
        statusStrings.put( "completed", N1QL_COMPLETED );
        statusStrings.put( "stopped", N1QL_STOPPED );
        statusStrings.put( "timeout", N1QL_TIMEOUT );
        statusStrings.put( "fatal", N1QL_FATAL );
    }

    String schema;
    String url;
    String user;
    String password;
    String credentials;
    String scanConsistency = "not_bounded";

    SQLWarning sqlWarning;

    Cluster cluster;
    boolean ssl=false;

    int connectTimeout=0;
    int queryTimeout=75;
    boolean readOnly = false;
    long updateCount;
    CBResultSet resultSet;
    List <String> batchStatements = new ArrayList<String>();

    public String getURL()
    {
        return url;
    }

    public String getUserName()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    public String getCredentials() { return credentials; }

    public void setReadOnly( boolean readOnly )
    {
        this.readOnly = readOnly;
    }

    public boolean getReadOnly( ) { return this.readOnly; }

    private static final Logger logger = LoggerFactory.getLogger(ProtocolImpl.class);

    CloseableHttpClient httpClient;

    private RequestConfig requestConfig;
	private BasicCredentialsProvider credsProvider;
	private HttpClientContext httpContext;

    public ProtocolImpl(String url, Properties props)
    {

        if ( props.containsKey(ConnectionParameters.USER))
        {
            user=props.getProperty(ConnectionParameters.USER);
        }
        if (props.containsKey(ConnectionParameters.PASSWORD))
        {
            password=props.getProperty(ConnectionParameters.PASSWORD);
        }
        if (props.containsKey("credentials"))
        {
            credentials = props.getProperty("credentials");
        }

		if ((user != null) && (password != null)) {
			this.credsProvider = new BasicCredentialsProvider();
			credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            
            AuthCache authCache = new BasicAuthCache();
            
            // Add AuthCache to the execution context
            httpContext = HttpClientContext.create();
            httpContext.setCredentialsProvider(credsProvider);
            httpContext.setAuthCache(authCache);
		}

		this.url = url;
        setConnectionTimeout(props.getProperty(ConnectionParameters.CONNECTION_TIMEOUT));
        if (props.containsKey(ConnectionParameters.SCAN_CONSISTENCY))
        {
            scanConsistency=props.getProperty(ConnectionParameters.SCAN_CONSISTENCY);
        }

        requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(0)
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(connectTimeout)
                .build();

        if (props.containsKey(ConnectionParameters.ENABLE_SSL) && props.getProperty(ConnectionParameters.ENABLE_SSL).equals("true"))
        {
            SSLContextBuilder builder = SSLContexts.custom();

            try
            {
                builder.loadTrustMaterial(null, new TrustStrategy() {
                    @Override
                    public boolean isTrusted(X509Certificate[] chain, String authType)
                            throws CertificateException {
                        return true;
                    }
                });
                SSLContext sslContext = builder.build();
                SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                        sslContext, new X509HostnameVerifier() {
                    @Override
                    public void verify(String host, SSLSocket ssl)
                            throws IOException {
                    }

                    @Override
                    public void verify(String host, X509Certificate cert)
                            throws SSLException {
                    }

                    @Override
                    public void verify(String host, String[] cns,
                                       String[] subjectAlts) throws SSLException {
                    }

                    @Override
                    public boolean verify(String s, SSLSession sslSession) {
                        return true;
                    }
                });

                Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder
                        .<ConnectionSocketFactory> create().register("https", sslsf)
                        .build();
                HttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
                HttpClientBuilder httpClientBuilder = HttpClients.custom().setConnectionManager(cm).setDefaultRequestConfig(requestConfig);
                httpClient = httpClientBuilder.build();
                ssl=true;

            }catch (Exception ex)
            {
                logger.error("Error creating ssl client", ex);
            }



        }
        else
        {
        	HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig);
            httpClient = httpClientBuilder.build();
        }
    }

    public void connect() throws Exception
    {

        pollCluster();

    }

    private void rewriteURLs(Map<String, String> m, String sourceHost) {
        for (String key: m.keySet()) {
            String val = m.get(key);
            if (val != null && val.startsWith("http")) {
                try {
                    URL cur = new URL(val);
                    if (cur.getHost().equals("127.0.0.1")) {
                        URL revisedURL = new URL(
                                cur.getProtocol(),
                                sourceHost,
                                cur.getPort(),
                                cur.getFile());
                        m.put(key, revisedURL.toString());
                    }
                } catch (MalformedURLException e) {
                    // Not a real URL. Do nothing. Keep going.
                    continue;
                }
            }
        }
    }

    // The URLs in the JSON array may contain 127.0.0.1-based addresses.
    // If they do, we rewrite them based on the host address we used to fetch the data.
    private void rewriteURLs(List<Map> jsonArray) throws IOException {
        URL sourceURL = new URL(url);
        String sourceHost = sourceURL.getHost();
        for (Map m : jsonArray) {
           rewriteURLs(m, sourceHost);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Cluster handleClusterResponse(CloseableHttpResponse response) throws IOException
    {
        int status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        String string = EntityUtils.toString(entity);
        logger.trace ("Cluster response {}", string);

        // has to be an object here since we can get a 404 back which is a string
        Object jsonArray = gson.fromJson(string, listOfMap);

        String message="";

        switch (status)
        {
            case 200:
                //noinspection unchecked
                rewriteURLs((List<Map>)jsonArray);
                return new Cluster((List)jsonArray, ssl);
            case 400:
                message = "Bad Request";
                break;
            case 401:
                message = "Unauthorized Request credentials are missing or invalid";
                break;
            case 403:
                message = "Forbidden Request: read only violation or client unauthorized to modify";
                break;
            case 404:
                message = "Not found: Check the URL";
                break;
            case 405:
                message = "Method not allowed: The REST method type in request is supported";
                break;
            case 409:
                message = "Conflict: attempt to create a keyspace or index that already exists";
                break;
            case 410:
                message = "Gone: The server is doing a graceful shutdown";
                break;
            case 500:
                message = "Internal server error: unforeseen problem processing the request";
                break;
            case 503:
                message = "Service Unavailable: there is an issue preventing the request from being serv serviced";
                break;
        }
        throw new ClientProtocolException(message +": " + status);

    }

    public CBResultSet query(CBStatement statement, String sql) throws SQLException
    {

        Instance instance = getNextEndpoint();


        @SuppressWarnings("unchecked") Map <String,String>parameters = new HashMap();

        parameters.put(STATEMENT,sql);
        addOptions(parameters);

        List<NameValuePair>parms = new ArrayList<NameValuePair>();

        for(String parameter:parameters.keySet())
        {
            parms.add(new BasicNameValuePair(parameter, String.valueOf(parameters.get(parameter))));
        }


        while(true)
        {
            String url = instance.getEndpointURL(ssl);
            logger.trace("Using endpoint {}", url);

            URI uri = null;
            try
            {
                uri = new URIBuilder(url).addParameters(parms).build();
            } catch (URISyntaxException ex) {
                logger.error("Invalid request {}", url);
            }

            HttpGet httpGet = new HttpGet(uri);

            httpGet.setHeader("Accept", "application/json");
            logger.trace("Get request {}", httpGet.toString());

            try {
                CloseableHttpResponse response = httpClient.execute(httpGet, httpContext);

                return new CBResultSet(statement, handleResponse(sql, response));

            } catch (ConnectTimeoutException cte)
            {
                logger.trace(cte.getLocalizedMessage());

                // this one failed, lets move on
                invalidateEndpoint(instance);
                // get the next one
                instance = getNextEndpoint();
                if (instance == null) {
                    throw new SQLException("All endpoints have failed, giving up");
                }


            } catch (IOException ex) {
                logger.error("Error executing query [{}] {}", sql, ex.getMessage());
                throw new SQLException("Error executing update", ex.getCause());
            }
        }
    }


    public int executeUpdate(CBStatement statement, String query) throws SQLException
    {
        boolean hasResultSet = execute(statement, query);
        if (!hasResultSet)
        {
            return (int)getUpdateCount();
        }
        else
        {
            return 0;
        }

    }

    public CouchResponse handleResponse(String sql, CloseableHttpResponse response) throws SQLException,IOException {
        int status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();

        CouchResponse couchResponse = new CouchResponse();

        String strResponse = EntityUtils.toString(entity);
//        logger.trace( "Response to query {} {}", sql, strResponse );

        Object foo = gson.fromJson(strResponse, Map.class);
        Map<String, Object> rootAsMap = null;
        if (foo instanceof Map)
        {
            //noinspection unchecked
            rootAsMap = (Map <String,Object>) foo;
        }
        else
        {
            logger.debug("error");
        }
        couchResponse.status    = (String)rootAsMap.get("status");
        couchResponse.requestId = (String)rootAsMap.get("requestID");
        Object signature = rootAsMap.get("signature");

        if ( signature instanceof Map )
        {
            //noinspection unchecked
            couchResponse.signature = (Map)signature;
            //noinspection unchecked
            couchResponse.results   = (List)rootAsMap.get("results");
        }
        else if ( signature instanceof String )
        {
            couchResponse.signature =  new HashMap<String, String>();
            couchResponse.signature.put("$1",(String)signature);

            Iterator iterator = ((List)rootAsMap.get("results")).iterator();

            couchResponse.results=new ArrayList<>();
            while ( iterator.hasNext() )
            {
                Object object = iterator.next();

                HashMap entry = new HashMap();
                //noinspection unchecked
                entry.put("$1", object );
                //noinspection unchecked
                couchResponse.results.add(entry) ;
            }

        }
        else if (signature != null)
        {
            throw new SQLException("Error reading signature" + signature );
        }
        //noinspection unchecked
        couchResponse.metrics   = gson.fromJson(gson.toJsonTree((Map)rootAsMap.get("metrics")), CouchMetrics.class);
        List errorList = (List)rootAsMap.get("errors");
        if ( errorList != null )
        {
            //noinspection unchecked,unchecked
            couchResponse.errors  = gson.fromJson(gson.toJsonTree(errorList), listOfCouchError);
        }
        List  warningList = (List)rootAsMap.get("warnings");
        if ( warningList != null )
        {
            //noinspection unchecked,unchecked
            couchResponse.warnings  = gson.fromJson(gson.toJsonTree(warningList), listOfCouchError);

            for (CouchError warning : couchResponse.warnings)
            {
                if ( sqlWarning != null )
                {
                    sqlWarning = new SQLWarning(warning.msg,null, warning.code);
                }
                else
                {
                    sqlWarning.setNextWarning(new SQLWarning(warning.msg,null, warning.code));
                }
            }
        }


        //JsonObject jsonObject = jsonReader.readObject();
        //logger.trace( "response from query {} {}", sql, jsonObject.toString());


        //String statusString = (String)jsonObject.get("status");

        Integer iStatus = statusStrings.get(couchResponse.status);
        String message;

        switch (status)
        {
            case 200:
                switch (iStatus.intValue())
                {
                    case N1QL_ERROR:
                        List <CouchError> errors = couchResponse.errors;
                        throw new SQLException(errors.get(0).msg);

                    case N1QL_SUCCESS:
                        return couchResponse;

                    case N1QL_COMPLETED:
                    case N1QL_FATAL:
                    case N1QL_RUNNING:
                    case N1QL_STOPPED:
                    case N1QL_TIMEOUT:
                        message = "Invalid Status";
                        fillSQLException(message, couchResponse);

                    default:
                        logger.error("Unexpected status string {} for query {}", couchResponse.status, sql);
                        throw new SQLException("Unexpected status: " + couchResponse.status );

                }
            case 400:
                message = "Bad Request";
                fillSQLException(message, couchResponse);
            case 401:
                message = "Unauthorized Request credentials are missing or invalid";
                fillSQLException(message, couchResponse);
            case 403:
                message = "Forbidden Request: read only violation or client unauthorized to modify";
                fillSQLException(message, couchResponse);
            case 404:
                message = "Not found: Request references an invalid keyspace or there is no primary key";
                fillSQLException(message, couchResponse);
            case 405:
                message = "Method not allowed: The REST method type in request is supported";
                fillSQLException(message, couchResponse);
            case 409:
                message = "Conflict: attempt to create a keyspace or index that already exists";
                fillSQLException(message, couchResponse);
            case 410:
                message = "Gone: The server is doing a graceful shutdown";
                fillSQLException(message, couchResponse);
            case 500:
                message = "Internal server error: unforeseen problem processing the request";
                fillSQLException(message, couchResponse);
            case 503:
                message = "Service Unavailable: there is an issue preventing the request from being serviced";
                logger.debug("Error with the request {}", message);

                CouchError  errors, warnings;

                if (couchResponse.metrics.errorCount > 0 )
                {
                    errors= couchResponse.errors.get(0);
                    logger.error("Error Code: {} Message: {} for query {} ",errors.code,errors.msg, sql);
                }
                if ( couchResponse.metrics.warningCount > 0 )
                {
                    warnings = couchResponse.warnings.get(0);
                    logger.error("Warning Code: {} Message: {} for query {}",warnings.code, warnings.msg, sql);
                }


                fillSQLException(message, couchResponse);

            default:
                throw new ClientProtocolException("Unexpected response status: " + status);

        }
    }

    private void fillSQLException(String msg, CouchResponse response)  throws SQLException
    {
        CouchError error ;
        if ( response.metrics.errorCount > 0 )
        {
            error = response.errors.get(0);

        }
        else if ( response.metrics.warningCount > 0)
        {
            error = response.errors.get(0);
        }
        else
        {
            throw new SQLException(msg);
        }
        throw new SQLException(error.msg, null, error.code);
    }


    public CouchResponse doQuery(String query, Map queryParameters) throws SQLException
    {
        Instance endPoint = getNextEndpoint();

        // keep trying endpoints
        while(true)
        {

            try {
                String url = endPoint.getEndpointURL(ssl);

                logger.trace("Using endpoint {}", url);
                HttpPost httpPost = new HttpPost(url);
                httpPost.setHeader("Accept", "application/json");

                logger.trace("do query {}", httpPost.toString());
                addOptions(queryParameters);


                String jsonParameters = gson.toJson(queryParameters);
                StringEntity entity = new StringEntity(jsonParameters, ContentType.APPLICATION_JSON);


                httpPost.setEntity(entity);

                CloseableHttpResponse response = httpClient.execute(httpPost, httpContext);

                return handleResponse(query, response);

            }
            catch (ConnectTimeoutException cte)
            {
                logger.trace(cte.getLocalizedMessage());

                // this one failed, lets move on
                invalidateEndpoint(endPoint);
                // get the next one
                endPoint = getNextEndpoint();
                if (endPoint == null) {
                    throw new SQLException("All endpoints have failed, giving up");
                }


            } catch (Exception ex) {
                logger.error("Error executing query [{}] {}", query, ex.getMessage());
                throw new SQLException("Error executing update", ex);
            }
        }
    }

	public boolean execute(CBStatement statement, String query) throws SQLException
    {
        try
        {
            Map parameters = new HashMap();

//            nameValuePairs.add(new BasicNameValuePair("pretty","0"));
            //noinspection unchecked
            parameters.put(STATEMENT, query);

            // do the query
            CouchResponse response = doQuery(query, parameters );

            updateCount = response.metrics.mutationCount;
            if ( updateCount > 0 )
            {
                return false;
            }

            // no sense creating the object if it is false
            if ( response.metrics.resultCount == 0 ) return false;
            resultSet = new CBResultSet(statement, response);
                return true;


        }
        catch (Exception ex)
        {
            logger.error ("Error executing update query {} {}", query, ex.getMessage());
            throw new SQLException("Error executing update",ex.getCause());
        }
    }

    public CouchResponse prepareStatement( String sql, String []returning ) throws SQLException
    {
        Map parameters = new HashMap();

//            nameValuePairs.add(new BasicNameValuePair("pretty","0"));


        // append returning clause
        if (returning != null )
        {
            sql += " RETURNING ";

            // loop through column names
            for (int i=0; i< returning.length;)
            {
                // add the column
                sql+=returning[i++];
                // add the , if not the last one
                if (i < returning.length) sql += ',';
            }
        }

        //noinspection unchecked
        parameters.put(STATEMENT, "prepare " + sql);

        return doQuery(sql, parameters);
    }

// batch statements do not work
    public int [] executeBatch() throws SQLException
    {
        try
        {
            Instance instance = getNextEndpoint();
            String url = instance.getEndpointURL(ssl);

            HttpPost httpPost = new HttpPost( url );
            httpPost.setHeader("Accept", "application/json");

            Map <String,Object> parameters = new HashMap<String,Object>();
            addOptions(parameters);
            for (String query:batchStatements)
            {
                parameters.put(STATEMENT, query);
            }

            CloseableHttpResponse response = httpClient.execute(httpPost, httpContext);
            int status = response.getStatusLine().getStatusCode();

            if ( status >= 200 && status < 300 )
            {
                HttpEntity entity = response.getEntity();
                String string = EntityUtils.toString(entity);
                logger.trace ("Batch response {}", string);

                // has to be an object here since we can get a 404 back which is a string
                Map <String,Object> jsonObject = gson.fromJson(string, listOfMap);

                String statusString = (String)jsonObject.get("status");

                if (statusString.equals("errors"))
                {
                    List errors= (List)jsonObject.get("errors");
                    Map error = (Map)errors.get(0);
                    throw new SQLException((String)error.get("msg"));
                }
                else if (statusString.equals("success"))
                {
                    Map  metrics = (Map)jsonObject.get("metrics");
                    if ( metrics.containsKey("mutationCount") )
                    {
                        updateCount = (int)metrics.get("mutationCount");
                        return new int [0];
                    }
                    if ( metrics.containsKey("resultCount") )
                    {
                       // TODO FIX ME resultSet = new CBResultSet(jsonObject);
                        return new int [0];
                    }
                }
                else if (statusString.equals("running"))
                {
                    return new int [0];
                }
                else if (statusString.equals("completed"))
                {
                    return new int [0];
                }
                else if (statusString.equals("stopped"))
                {
                    return new int [0];
                }
                else if (statusString.equals("timeout"))
                {
                    return new int [0];
                }
                else if (statusString.equals("fatal"))
                {
                    return new int [0];
                }

                else
                {
                    //logger.error("Unexpected status string {} for query {}", statusString, query);
                    throw new SQLException("Unexpected status: " + statusString);
                }
            }

            else
            {
                throw new ClientProtocolException("Unexpected response status: " + status);
            }
        }
        catch (Exception ex)
        {
            //logger.error ("Error executing update query {} {}", query, ex.getMessage());
            throw new SQLException("Error executing update",ex.getCause());
        }
        return new int [0];

    }
    public void addBatch( String query ) throws SQLException
    {
        batchStatements.add(query);
    }
    public void clearBatch()
    {
        batchStatements.clear();
    }
    public long getUpdateCount()
    {
        return updateCount;
    }
    public CBResultSet getResultSet()
    {
        return resultSet;
    }

    public void setConnectionTimeout(String timeout)
    {
        if (timeout!=null)
        {
            connectTimeout=Integer.parseInt(timeout);
        }
    }
    public void setConnectionTimeout(int timeout )
    {
        connectTimeout=timeout;
    }

    public void setQueryTimeout( int seconds ) throws SQLException
    {
        this.queryTimeout =  seconds;
    }
    public int getQueryTimeout( ) throws SQLException
    {
        return queryTimeout;
    }
    public void close() throws Exception
    {
        httpClient.close();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        return sqlWarning;
    }

    @Override
    public void clearWarning() throws SQLException
    {
       sqlWarning=null;
    }

    @Override
    public void setSchema(String schema) throws SQLException
    {
        if (schema != null && schema.compareToIgnoreCase("system")==0)
        {
            schema='#'+schema;
        }
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException
    {

        if (schema!= null && schema.startsWith("#"))
        {
            return schema.substring(1);
        }
        else {
            return schema;
        }
    }


    private void addOptions(Map parameters)
    {

        //noinspection unchecked
        parameters.put(ENCODING,"UTF-8");

        if ( schema != null )
        {
            //noinspection unchecked
            parameters.put(NAMESPACE, schema);
        }
        if( readOnly )
        {
            //noinspection unchecked
            parameters.put(READ_ONLY, true);
        }
        if ( queryTimeout != 0 )
        {
            //noinspection unchecked
            parameters.put(TIMEOUT, ""+queryTimeout+'s');
        }

        if (credentials != null)
        {
            //noinspection unchecked
            parameters.put(CREDENTIALS, credentials);
        }

        //noinspection unchecked
        parameters.put(SCAN_CONSITENCY,scanConsistency);

    }

    public boolean isValid(int timeout)
    {


        String query = "select 1";

        Map parameters = new HashMap();
        //noinspection unchecked
        parameters.put(STATEMENT, query);
        // do the query

        try
        {
            CouchResponse response = doQuery(query, parameters);
            return response.getMetrics().getResultCount() == 1;
        }
        catch (Exception ex)
        {
            return false;

        }
    }

    // all access to clusters has to be synchronized as there is a thread
    // changing the value
    // see CBDriver.ClusterThread

    AtomicBoolean clusterSynch = new AtomicBoolean(true);
    public Cluster getCluster()
    {
        Cluster ret=null;
        // loop waiting for access
        while (clusterSynch.getAndSet(false)){}

        ret=cluster;
        clusterSynch.set(true);
        return ret;
    }
    public void setCluster(Cluster cluster)
    {
        while(clusterSynch.getAndSet(false)){}

        this.cluster = cluster;

        rebuildAuthCache();

        clusterSynch.set(true);
    }

    public Instance getNextEndpoint()
    {
        Instance instance = null;
        while(clusterSynch.getAndSet(false)){}

        instance = cluster.getNextEndpoint();

        clusterSynch.set(true);
        return instance;
    }
    public void invalidateEndpoint(Instance instance)
    {
        while(clusterSynch.getAndSet(false)){}

        cluster.invalidateEndpoint(instance);
        clusterSynch.set(true);
    }
    public void pollCluster() throws SQLException
    {
        HttpGet httpGet = new HttpGet(url+"/admin/clusters/default/nodes");
        httpGet.setHeader("Accept", "application/json");

        try
        {
            CloseableHttpResponse httpResponse = httpClient.execute(httpGet, httpContext);

            setCluster(handleClusterResponse(httpResponse));
        }
        catch(Exception ex)
        {
            logger.error("Error opening connection {}", ex.getMessage());

            throw new SQLException("Error getting cluster response", ex);}

    }

	private void rebuildAuthCache() {
		AuthCache authCache = httpContext.getAuthCache();
		authCache.clear();

		for (Instance endPoint : cluster.getEndpoints()) {
			String url = endPoint.getEndpointURL(ssl);

			logger.trace("Using endpoint {}", url);
			URI uri = null;
			try {
				uri = new URI(url);
			} catch (URISyntaxException ex) {
				logger.error("Invalid endpoint {}", url);
			}

			HttpHost targetHost = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
			authCache.put(targetHost, new BasicScheme());
		}
	}
/*
    public static void main(String[] args) throws ClientProtocolException, IOException, SQLException {
    	Properties prop = new Properties();
    	prop.put("user", "admin");
    	prop.put("password", "test");
    	prop.put("EnableSSL", "true");
    	ProtocolImpl p = new ProtocolImpl("http://localhost", prop);
    	
    	String uri = "https://localhost:18093/query/service?readonly=true&statement=select+doc.*+from+%60gluu_user%60+doc+where+%28%28uid+%3D+%22mbaser%22%29+OR+%28%22mbaser%22+IN+uid%29%29+OR+%28%28mail+%3D+%22mbaser%22%29+OR+%28%22mbaser%22+IN+mail%29%29&encoding=UTF-8&timeout=5000s&scan_consistency=not_bounded";
        HttpGet httpGet = new HttpGet(uri);

        httpGet.setHeader("Accept", "application/json");
		String url = "https://localhost:18093";

		URI uri2 = null;
		try {
			uri2 = new URI(url);
		} catch (URISyntaxException ex) {
		}

		HttpHost targetHost = new HttpHost(uri2.getHost(), uri2.getPort(), uri2.getScheme());
		p.httpContext.getAuthCache().put(targetHost, new BasicScheme());

		
        CloseableHttpResponse response = p.httpClient.execute(httpGet, p.httpContext);
        CouchResponse resp = p.handleResponse("", response);
        System.out.println(resp);
        System.out.println(response.getEntity());
	}
*/
}


