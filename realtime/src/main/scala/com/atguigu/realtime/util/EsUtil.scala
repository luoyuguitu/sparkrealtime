package com.atguigu.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object EsUtil {
  var factory: JestClientFactory = null

  def build() = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop108:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(1000)
      .readTimeout(1000)
      .build())
  }

  def getClient: JestClient = {
    if (factory == null) {
      build()
    }
    factory.getObject
  }

  def bulkDoc(sourceList: List[Any], indexName: String): Unit = {
    if (sourceList != null && sourceList.size > 0) {
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for (elem <- sourceList) {
        val index: Index = new Index.Builder(elem).index(indexName).`type`("_doc").build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES:"+items.size()+"条数")
      jest.close()
    }
  }
}
