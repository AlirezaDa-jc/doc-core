import weaviate
import json

client = weaviate.connect_to_local()

questions = client.collections.get("files_db")

response = questions.query.near_text(
    query="AWARENESS AND TRAINING (AT) Lvl2",
    limit=10
)

# response = questions.generate.near_text(
#     query="AWARENESS AND TRAINING (AT) Lvl2",
#     grouped_task="AWARENESS AND TRAINING (AT) Lvl2",
#     limit=2
# )
print(response)
# for obj in response.objects:
#     print(json.dumps(obj.properties, indent=2))

client.close()  # Free up resources