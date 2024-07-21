from elasticsearch import Elasticsearch

class ElasticsearchWrapper:
    def __init__(self):
        self.es = Elasticsearch(["http://localhost:9200"])

    def get_all_documents(self, index_name):
        headers = {'Content-Type': 'application/json'}
        try:
            res = self.es.search(index=index_name, body={"query": {"match_all": {}}}, headers=headers)
            return [doc["_source"] for doc in res['hits']['hits']]
        except Exception as e:
            raise e

    def create_index(self, index_name, settings=None):
        self.es.indices.create(index=index_name, body=settings or {})

    def create_document(self, index_name, doc_id, document):
        headers = {'Content-Type': 'application/json'}
        return self.es.index(index=index_name, id=doc_id, body=document, headers=headers)

    def get_document(self, index_name, doc_id):
        headers = {'Content-Type': 'application/json'}
        try:
            return self.es.get(index=index_name, id=doc_id, headers=headers)["_source"]
        except:
            return None

    def update_document(self, index_name, doc_id, document):
        headers = {'Content-Type': 'application/json'}
        return self.es.update(index=index_name, id=doc_id, body={"doc": document}, headers=headers)

    def delete_document(self, index_name, doc_id):
        headers = {'Content-Type': 'application/json'}
        return self.es.delete(index=index_name, id=doc_id, headers=headers)
