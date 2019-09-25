package com.hujie.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class TestEs {
    @Test
    public void testES() throws UnknownHostException, ExecutionException, InterruptedException {
        //第一个参数时ip  第二个是端口
        TransportAddress transportAddress = new TransportAddress( InetAddress.getByName( "192.168.137.70" ), 9300 );
        //使用transport完成和es的交互
        TransportClient transportClient = new PreBuiltTransportClient( Settings.EMPTY ).addTransportAddress( transportAddress );
        //新建索引
        /*CreateIndexResponse ems = transportClient.admin().indices().prepareCreate( "dangdang" ).execute().get();
        System.out.println(ems);*/

        //删除索引
        DeleteIndexResponse indexResponse = transportClient.admin().indices().prepareDelete( "dangdang" ).execute().get();
        System.out.println( indexResponse );
    }


    //创建索引并创建类型同时指定映射
    @Test
    public void testCreateIndexAndTypeAndMapping() throws Exception {
        TransportClient transportClient = new PreBuiltTransportClient( Settings.EMPTY ).addTransportAddress( new TransportAddress( InetAddress.getByName( "192.168.137.70" ), 9300 ) );
        System.out.println( "创建index" );
        CreateIndexResponse indexResponse = transportClient.admin().indices().prepareCreate( "dangdang" ).execute().get();
        System.out.println( indexResponse.index() );

        System.out.println( "====创建类型指定映射===" );
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject()
                .startObject( "properties" )
                .startObject( "name" )
                .field( "type", "text" )
                .field( "analyzer", "ik_max_word" )
                .endObject()
                .startObject( "age" )
                .field( "type", "integer" )
                .endObject()
                .startObject( "sex" )
                .field( "type", "keyword" )
                .endObject()
                .startObject( "content" )
                .field( "type", "text" )
                .field( "analyzer", "ik_max_word" )
                .endObject()
                .endObject().endObject();
        PutMappingRequest putMappingRequest = new PutMappingRequest( "dangdang" ).type( "book" ).source( mappingBuilder );
        transportClient.admin().indices().putMapping( putMappingRequest ).get();
    }

    //索引一条记录
    @Test
    public void testIndex() throws IOException {
        TransportClient transportClient = new PreBuiltTransportClient( Settings.EMPTY ).addTransportAddress( new TransportAddress( InetAddress.getByName( "192.168.137.70" ), 9300 ) );
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field( "name", "胡杰" )
                .field( "age", 24 )
                .field( "sex", "男" )
                .field( "content", "他是一个有梦想的中国人，他要努力赚钱，过上好日子" ).endObject();
        IndexResponse indexResponse = transportClient.prepareIndex( "dangdang", "book" ).setSource( xContentBuilder ).get();
        System.out.println( indexResponse.status() );
    }


    //更新一条索引
    @Test
    public void testPost() throws IOException {
        TransportClient transportClient = new PreBuiltTransportClient( Settings.EMPTY ).addTransportAddress( new TransportAddress( InetAddress.getByName( "192.168.137.70" ), 9300 ) );
        XContentBuilder sources = XContentFactory.jsonBuilder();
        sources.startObject().field( "name", "小黑" )
                .endObject();
        UpdateResponse updateResponse = transportClient.prepareUpdate( "dangdang", "book", "75Dz8WwBev3Fou2NAhAk" ).setDoc( sources ).get();
        System.out.println( updateResponse.status() );

    }


    //删除一条索引
    @Test
    public void testDeleteIndex() throws IOException {
        TransportClient transportClient = new PreBuiltTransportClient( Settings.EMPTY ).addTransportAddress( new TransportAddress( InetAddress.getByName( "192.168.137.70" ), 9300 ) );
        DeleteResponse deleteResponse = transportClient.prepareDelete( "dangdang", "book", "75Dz8WwBev3Fou2NAhAk" ).get();
        System.out.println( deleteResponse.status() );

    }


    //批量更新    略
    @Test
    public void testupdateIds() throws UnknownHostException {
        TransportClient transportClient = new PreBuiltTransportClient( Settings.EMPTY ).addTransportAddress( new TransportAddress( InetAddress.getByName( "192.168.137.70" ), 9300 ) );

    }


    //检索记录
    //查询所有并排序
    @Test
    public void testSearch() throws UnknownHostException {
        TransportClient transportClient = new PreBuiltTransportClient( Settings.EMPTY ).addTransportAddress( new TransportAddress( InetAddress.getByName( "192.168.137.70" ), 9300 ) );
        SearchResponse searchResponse = transportClient.prepareSearch( "dangdang" ).setTypes( "book" ).setQuery( QueryBuilders.matchAllQuery() ).addSort( "age", SortOrder.DESC ).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("符合条件的记录数："+hits.totalHits);
        for (SearchHit hit : hits) {
            System.out.println("当前索引的分数："+hit.getScore());
            System.out.println("对应结果===="+hit.getSourceAsString());
            System.out.println("指定字段结果："+hit.getSourceAsMap().get( "name" ));
        }
    }

}
