from dotenv import load_dotenv
from common.genie_logger import GenieLogger
from common.utils import env_utils, str_utils
from pinecone import Pinecone
from langchain.text_splitter import RecursiveCharacterTextSplitter
from azure.ai.inference import EmbeddingsClient
from azure.core.credentials import AzureKeyCredential

from pinecone import Pinecone


load_dotenv()
logger = GenieLogger()

endpoint = env_utils.get("AZURE_INFERENCE_ENDPOINT")
credential = env_utils.get("AZURE_INFERENCE_CREDENTIAL")

if not endpoint or not credential:
    logger.error(f"Endpoint or Credential missing: Endpoint={endpoint}, Credential={credential}")
    raise ValueError("Azure endpoint or credential is missing.")

DEV_MODE = env_utils.get("DEV_MODE", "")

PINECONE_API_KEY = env_utils.get("PINECONE_API_KEY")
PINECONE_INDEX = env_utils.get(DEV_MODE + "PINECONE_INDEX")

from azure.ai.inference import ChatCompletionsClient
from azure.core.credentials import AzureKeyCredential

embeddings_model = EmbeddingsClient(
    endpoint=endpoint,
    credential=AzureKeyCredential(credential),
)
model_name = "intfloat/multilingual-e5-large"
pc = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(PINECONE_INDEX)


class GenieEmbeddingsClient:
    def __init__(self):
        self.api_key = env_utils.get("LANGSMITH_API_KEY")

        if not self.api_key:
            raise ValueError("LangSmith API key is missing. Please set it in the .env file.")

    def embed_document(self, doc_text, metadata):
        try:
            logger.info("Begin embedding document")
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
            chunks = text_splitter.split_text(doc_text)

            embeddings = self.generate_embeddings(chunks)
            vector_id = metadata["id"]
            user = metadata.get("user")
            domain = str_utils.get_email_suffix(user)

            # correct_metadata = {
            #     "user": metadata.get("user"),
            #     "domain": domain,
            #     "tenant_id": metadata.get("tenant_id"),
            #     "type": metadata.get("type"),
            # }
            correct_metadata = metadata
            correct_metadata["domain"] = domain
            # embeddings_data = [embedding["embedding"] for embedding in embeddings]
            ids = [f"{vector_id}_{i}" for i in range(len(embeddings))]
            pinecone_metadata = [{**correct_metadata, "chunk": chunk} for chunk in chunks]

            index.upsert(vectors=list(zip(ids, embeddings, pinecone_metadata)))
            logger.info("Finished embedding document")
            return True
        except Exception as e:
            logger.error(f"Error in embedding document: {e}")
            return False

    def generate_embeddings(self, text: list[str]):
        if not text:
            return []
        if isinstance(text, str):
            text = [text]
        response = embeddings_model.embed(input=text)
        response_data = response.data
        embeddings_data = [
            [float(value) for value in embedding["embedding"]]  # Ensure all values are floats
            for embedding in response_data
        ]
        return embeddings_data

    def search_materials_by_prospect_data(self, user_id, prospect_data):
        profile_query = f"""
            What are the best materials to use in order generate to sell to that person. Any information regarding.
            Any information, regarding my product, my use cases, my competitve landscape,etc..  would be highly beneficial.
            Here's the prospect data: {prospect_data}
        """
        return self.search_by_query_and_user(profile_query, user_id)
    
    def search_files_by_action_item(self, user_id, action_item):
        action_item_query = f"""
            What are the best matierlas that comply with the following action item: {action_item}.
            The goal is to generate to sell to that person. Any information regarding the specific action item is highly beneficial.
            Here's the action item: {action_item}
        """
        return self.search_files_by_query_and_user(action_item_query, user_id, top_k=1)
    
    def search_files_by_query_and_user(self, query_text, user_id, top_k=5):
        results = self.query_embeddings(user_id, query_text, top_k=top_k)
        closest_file = None
        closest_file_text = None
        if results:
            logger.info(f"Returned {len(results['matches'])} embedding vectors for user {user_id}")
            chunks = results["matches"]
            for chunk in chunks:
                if chunk["metadata"] and chunk["metadata"]["file_name"]:
                    closest_file = chunk["metadata"]["file_name"]
                    closest_file_text = chunk["metadata"]["chunk"]
        else:
            logger.info(f"No results returned for user {user_id}")
        return closest_file, closest_file_text

    def search_by_query_and_user(self, query_text, user_id, top_k=5):
        results = self.query_embeddings(user_id, query_text, top_k=top_k)

        chunks_text = []
        if results:
            logger.info(f"Returned {len(results['matches'])} embedding vectors for user {user_id}")
            chunks = results["matches"]
            for chunk in chunks:
                chunks_text.append(chunk["metadata"]["chunk"])
        else:
            logger.info(f"No results returned for user {user_id}")
        return chunks_text
    
    def query_embeddings(self, user_id, query_text, top_k=5):
        query_embedding = self.generate_embeddings(query_text)
        if not query_embedding:
            logger.info(f"Query embedding is empty for user {user_id}")
            return []
        domain = str_utils.get_email_suffix(user_id)
        results = None
        if domain:
            results = index.query(
                vector=query_embedding, top_k=top_k, include_metadata=True, filter={"domain": domain}
            )

        if not results or not results["matches"]:
            results = index.query(
                vector=query_embedding, top_k=top_k, include_metadata=True, filter={"user": user_id}
            )

        return results
    
    def delete_vectors_by_user(self, user_id):
        if user_id:
            logger.info(f"Deleting vectors for user {user_id}")
            filter_metadata = {"user": user_id} 
            response = index.query(
                vector=[0] * 1024, 
                filter=filter_metadata,
                top_k=100, 
                include_metadata=False, 
                include_values=False  
            )

            vector_ids = [match['id'] for match in response['matches']]

            if vector_ids:
                index.delete(ids=vector_ids)
                print(f"Deleted {len(vector_ids)} vectors with matching metadata.")
            else:
                print("No vectors found with the given metadata filter.")
            logger.info(f"Deleted vectors for user {user_id}")
        else:
            logger.error(f"User ID not provided for deletion")
    
# if __name__ == "__main__":
#     embeddings_client = GenieEmbeddingsClient()
#     user_id = "asaf@genieai.ai"
#     prospect_data = "The prospect is a CTO at a tech company"
#     action_item = "Follow up with the prospect"
#     print(f"Searching for materials for user {user_id} based on prospect data")
#     data = embeddings_client.search_files_by_action_item(user_id, action_item)
#     print(data)
