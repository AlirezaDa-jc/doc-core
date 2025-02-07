import weaviate
from weaviate.classes.config import Configure

client = weaviate.connect_to_local()
client.collections.delete('files_db')
questions = client.collections.create(
    name="files_db",
    vectorizer_config=Configure.Vectorizer.text2vec_ollama(
        api_endpoint="http://host.docker.internal:11434",
        model="all-minilm:l6-v2"
    ),
    # generative_config=Configure.Generative.ollama(
    #     api_endpoint="http://host.docker.internal:11434",
    #     model="deepseek-v2:latest"
    # )
)
print('Database Created')
client.close()
