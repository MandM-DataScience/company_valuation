import os
from configparser import ConfigParser
from typing import Any, List, Iterator

import tiktoken
from langchain.chains.summarize import load_summarize_chain
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.document_loaders.unstructured import UnstructuredBaseLoader
from langchain.chat_models import ChatOpenAI


class UnstructuredStringLoader(UnstructuredBaseLoader):
    """
    Uses unstructured to load a string
    Source of the string, for metadata purposes, can be passed in by the caller
    """

    def __init__(
        self, content: str, source: str = None, mode: str = "single",
        **unstructured_kwargs: Any
    ):
        self.content = content
        self.source = source
        super().__init__(mode=mode, **unstructured_kwargs)

    def _get_elements(self) -> List:
        from unstructured.partition.text import partition_text

        return partition_text(text=self.content, **self.unstructured_kwargs)

    def _get_metadata(self) -> dict:
        return {"source": self.source} if self.source else {}


def split_text_in_chunks(text, chunk_size=20000):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=100)
    chunks = text_splitter.split_text(text)
    return chunks


def split_doc_in_chunks(doc, chunk_size=20000):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=100)
    chunks = text_splitter.split_documents(doc)
    return chunks


def doc_summary(docs):
    print(f'You have {len(docs)} document(s)')

    num_words = sum([len(doc.page_content.split(' ')) for doc in docs])

    print(f'You have roughly {num_words} words in your docs')
    print()
    print(f'Preview: \n{docs[0].page_content.split(". ")[0]}')

# def get_section_summary(section_text):


if __name__ == '__main__':
    import mongodb
    from edgar_utils import company_from_cik

    parser = ConfigParser()
    _ = parser.read(os.path.join("credentials.cfg"))
    model = "gpt-3.5-turbo"
    llm = ChatOpenAI(model_name=model, openai_api_key=parser.get("open_ai", "api_key"))

    url = 'https://www.sec.gov/Archives/edgar/data/8818/000000881823000002/avy-20221231.htm'
    doc = mongodb.get_document("documents", url)
    parsed_doc = mongodb.get_document("parsed_documents", url)
    company = company_from_cik(doc["cik"])

    result = {"_id": doc["_id"],
              "name": company["name"],
              "ticker": company["ticker"],
              "form_type": doc["form_type"],
              "filing_date": doc["filing_date"]}

    for section_title, section_text in parsed_doc.items():

        # if no section to summarize, skip
        if section_title == "_id" or len(section_text) == 0:
            continue
        # chunks = split_in_chunks(section_text)

        string_loader = UnstructuredStringLoader(section_text)
        doc = string_loader.load()
        doc_summary(doc)
        docs = split_doc_in_chunks(doc)
        doc_summary(docs)

        chain = load_summarize_chain(llm, chain_type="refine", verbose=True)
        res = chain.run(docs)
        print(res)