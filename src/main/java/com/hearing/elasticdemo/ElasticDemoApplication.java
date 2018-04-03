package com.hearing.elasticdemo;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class ElasticDemoApplication {

	@Autowired
	private TransportClient client;

	@GetMapping("/")
	public String index() {
		return "index";
	}

	@GetMapping("/get/book/test")
	public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {
		if (id.isEmpty()) {
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		GetResponse result = this.client.prepareGet("book", "test", id).get();
		if (!result.isExists()) {
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity(result.getSource(), HttpStatus.OK);
	}

	@PostMapping("/add/book/test")
	public ResponseEntity add(@RequestParam(name = "name") String name) {
		try {
			XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name", name)
                    .endObject();
			IndexResponse result = this.client.prepareIndex("book", "test")
					                          .setSource(xContentBuilder).get();
			return new ResponseEntity(result.getId(), HttpStatus.OK);
		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@DeleteMapping("/delete/book/test")
	public ResponseEntity delete(@RequestParam("id") String id) {
		DeleteResponse result = this.client.prepareDelete("book", "test", id).get();
		return new ResponseEntity(result.getResult().toString(), HttpStatus.OK);
	}

	@PutMapping("/update/book/test")
	public ResponseEntity update(@RequestParam("id") String id,
								 @RequestParam(name = "name", required = false) String name) {
		UpdateRequest update = new UpdateRequest("book", "test", id);
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
			if (name != null) {
				builder.field("name", name);
			}
			builder.endObject();
			update.doc(builder);
		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		try {
			UpdateResponse result = this.client.update(update).get();
			return new ResponseEntity(result.getResult().toString(), HttpStatus.OK);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

//	@PostMapping("/query/book/test")
//	public ResponseEntity query(@RequestParam(name = "id", required = false) String id,
//								@RequestParam(name = "name", required = false) String name) {
//		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//		if (id != null) {
//			boolQueryBuilder.must(QueryBuilders.matchQuery("id", id));
//		}
//		if (name != null) {
//			boolQueryBuilder.must(QueryBuilders.matchQuery("name", name));
//		}
//		RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery()
//	}

	public static void main(String[] args) {
		SpringApplication.run(ElasticDemoApplication.class, args);
	}
}
