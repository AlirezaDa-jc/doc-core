import os
import weaviate
from weaviate.classes.config import Configure, DataType, Property
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import json
import gc
import fitz  # PyMuPDF
import docx  # python-docx
import time
import re

client = weaviate.connect_to_local()
newsDb = client.collections.get("files_db")
num_workers = 16


def split_text_into_chunks(text: str, max_chunk_size: int = 1000) -> list:
    """
    Split text into chunks based on paragraphs and natural breaks.
    Try to keep semantic units (like sentences) together.
    """
    # Split into paragraphs first
    paragraphs = text.split('\n\n')
    chunks = []
    current_chunk = ""

    for paragraph in paragraphs:
        # Clean the paragraph
        paragraph = paragraph.strip()
        if not paragraph:
            continue

        # If paragraph itself is too long, split by sentences
        if len(paragraph) > max_chunk_size:
            sentences = re.split(r'(?<=[.!?])\s+', paragraph)
            for sentence in sentences:
                if len(current_chunk) + len(sentence) <= max_chunk_size:
                    current_chunk += " " + sentence if current_chunk else sentence
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                    current_chunk = sentence
        else:
            if len(current_chunk) + len(paragraph) <= max_chunk_size:
                current_chunk += "\n\n" + paragraph if current_chunk else paragraph
            else:
                chunks.append(current_chunk.strip())
                current_chunk = paragraph

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


def process_pdf(file_path: str) -> list:
    """Extract and chunk PDF content."""
    try:
        doc = fitz.open(file_path)
        chunks = []

        for page in doc:
            # Extract text with layout preservation
            text = page.get_text("text")
            # Split into smaller chunks
            page_chunks = split_text_into_chunks(text)

            # Add metadata to each chunk
            for chunk_idx, chunk in enumerate(page_chunks):
                chunks.append({
                    "type": "text",
                    "content": chunk,
                    "metadata": {
                        "source": file_path,
                        "page": page.number + 1,
                        "chunk": chunk_idx + 1
                    }
                })

        return chunks
    except Exception as e:
        print(f"Error processing PDF {file_path}: {e}")
        return None


def process_file(file_path: str) -> list:
    """Extract content based on file type."""
    _, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()

    try:
        if file_extension in ['.txt', '.md']:
            with open(file_path, 'r') as f:
                content = f.read()
            chunks = split_text_into_chunks(content)
            return [{"type": "text", "content": chunk} for chunk in chunks]

        elif file_extension in ['.pdf']:
            return process_pdf(file_path)

        elif file_extension in ['.docx']:
            doc = docx.Document(file_path)
            content = "\n\n".join(para.text for para in doc.paragraphs)
            chunks = split_text_into_chunks(content)
            return [{"type": "text", "content": chunk} for chunk in chunks]

        else:
            print(f"Unsupported file type: {file_path}")
            return None

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def process_batch(chunks: list) -> None:
    """Process a batch of chunks with GPU optimization"""
    try:
        with newsDb.batch.dynamic() as batch:
            for chunk in chunks:
                properties = {
                    "content": chunk['content'],
                    "type": chunk['type']
                }
                # Add metadata if available
                if 'metadata' in chunk:
                    properties['metadata'] = chunk['metadata']
                batch.add_object(properties)
    except Exception as e:
        print(e)


def import_with_batching(file_path: str, batch_size: int = 128) -> None:
    """Import data with GPU and CPU optimization"""
    try:
        all_chunks = []
        for root, _, filenames in os.walk(file_path):
            for filename in filenames:
                file_path = os.path.join(root, filename)
                processed_chunks = process_file(file_path)
                if processed_chunks:
                    all_chunks.extend(processed_chunks)

        batches = [all_chunks[i:i + batch_size] for i in range(0, len(all_chunks), batch_size)]
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for batch in batches:
                futures.append(executor.submit(process_batch, batch))

            completed = 0
            for future in tqdm(futures, desc="Processing batches"):
                future.result()
                completed += batch_size
                if completed % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed
                    print(f"Rate: {rate:.2f} chunks/second")
                    print(f"Memory usage per chunk: {gc.get_count()}")

        elapsed_time = time.time() - start_time
        final_rate = len(all_chunks) / elapsed_time
        print(f"Finished processing {len(all_chunks)} chunks in {elapsed_time:.2f} seconds")
        print(f"Final average rate: {final_rate:.2f} chunks/second")

    except Exception as e:
        print(f"Error during import: {str(e)}")
        raise


if __name__ == "__main__":
    import_with_batching("./directory", batch_size=128)