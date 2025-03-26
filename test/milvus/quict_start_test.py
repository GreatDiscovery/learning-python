from pymilvus import MilvusClient
from pymilvus import model

# doc: https://milvus.io/docs/zh/quickstart.md

def main():
    # 1. 设置向量数据库
    client = MilvusClient("milvus_demo.db")

    if client.has_collection(collection_name="demo_collection"):
        client.drop_collection(collection_name="demo_collection")
    client.create_collection(
        collection_name="demo_collection",
        dimension=768,  # The vectors we will use in this demo has 768 dimensions
    )

    # 2. 使用embedding生成数据
    # If connection to https://huggingface.co/ failed, uncomment the following path
    # import os
    # os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

    # This will download a small embedding model "paraphrase-albert-small-v2" (~50MB).
    embedding_fn = model.DefaultEmbeddingFunction()

    # Text strings to search from.
    docs = [
        "Artificial intelligence was founded as an academic discipline in 1956.",
        "Alan Turing was the first person to conduct substantial research in AI.",
        "Born in Maida Vale, London, Turing was raised in southern England.",
    ]

    vectors = embedding_fn.encode_documents(docs)
    # The output vector has 768 dimensions, matching the collection that we just created.
    print("Dim:", embedding_fn.dim, vectors[0].shape)  # Dim: 768 (768,)

    # Each entity has id, vector representation, raw text, and a subject label that we use
    # to demo metadata filtering later.
    data = [
        {"id": i, "vector": vectors[i], "text": docs[i], "subject": "history"}
        for i in range(len(vectors))
    ]

    print("Data has", len(data), "entities, each with fields: ", data[0].keys())
    print("Vector dim:", len(data[0]["vector"]))

    # 3. 插入数据
    res = client.insert(collection_name="demo_collection", data=data)

    print(res)

    # 4. 语义搜索
    query_vectors = embedding_fn.encode_queries(["Who is Alan Turing?"])

    res = client.search(
        collection_name="demo_collection",  # target collection
        data=query_vectors,  # query vectors
        limit=2,  # number of returned entities
        output_fields=["text", "subject"],  # specifies fields to be returned
    )

    print(res)

    # 5. 带元数据过滤的向量搜索
    docs = [
        "Machine learning has been used for drug design.",
        "Computational synthesis with AI algorithms predicts molecular properties.",
        "DDR1 is involved in cancers and fibrosis.",
    ]
    vectors = embedding_fn.encode_documents(docs)
    data = [
        {"id": 3 + i, "vector": vectors[i], "text": docs[i], "subject": "biology"}
        for i in range(len(vectors))
    ]

    client.insert(collection_name="demo_collection", data=data)

    res = client.search(
        collection_name="demo_collection",
        data=embedding_fn.encode_queries(["tell me AI related information"]),
        filter="subject == 'biology'",
        limit=2,
        output_fields=["text", "subject"],
    )

    print(res)

if __name__ == '__main__':
    main()
