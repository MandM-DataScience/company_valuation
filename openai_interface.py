from configparser import ConfigParser

from langchain.callbacks import get_openai_callback
from langchain.chains.summarize import load_summarize_chain
from langchain.chat_models import ChatOpenAI
from summarizer import UnstructuredStringLoader, split_doc_in_chunks

import openai
import os
import json
import tiktoken


parser = ConfigParser()
_ = parser.read(os.path.join("credentials.cfg"))
openai.api_key = parser.get("open_ai", "api_key")

# This context message will be included in every request
INITIAL_CONTEXT_MESSAGE = {"role": "system",
                           "content": "Act as an assistant for security analysis. Your goal is to help make sense of "
                                      "financial information available for US public companies on EDGAR."}
MODEL_MAX_TOKENS = {
    "gpt-3.5-turbo": 4097,
    "gpt-3.5-turbo-16k": 16384,
}


def get_completion(messages, model="gpt-3.5-turbo"):
    """
    Make a request to openAI model 'model'
    :param messages: list of messages to be used as input for the model
    :param model: model to use
    :return: model response
    """
    return openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0, # this is the degree of randomness of the model's output
    )


def num_tokens_from_messages(messages, model="gpt-3.5-turbo"):
    """
    Counts number of tokens in messages (in order to manage input size and compute API costs)
    :param messages:
    :param model:
    :return: number of tokens
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")

    if model in ["gpt-3.5-turbo", "gpt-3.5-turbo-16k"]:
        num_tokens = 0
        for message in messages:
            num_tokens += 4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
            for key, value in message.items():
                num_tokens += len(encoding.encode(value))
                if key == "name":  # if there's a name, the role is omitted
                    num_tokens += -1  # role is always required and always 1 token

        num_tokens += 2  # every reply is primed with <im_start>assistant
        return num_tokens


def compute_cost(tokens, model="gpt-3.5-turbo"):
    """
    Compute API cost from number of tokens
    :param tokens:
    :param model:
    :return: cost in USD
    """
    if model == "gpt-3.5-turbo":
        return round(tokens / 1000 * 0.002, 4)
    if model == "gpt-3.5-turbo-16k":
        return round(tokens / 1000 * 0.004, 4)


def get_text_tokens(value, model="gpt-3.5-turbo"):
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(value))


def get_messages(company_name, ticker, exchange, form, filing_date, section_title, section_text):
    # TODO Further refine prompt by trial and error on multiple filings and multiple companies
    prompt = f"I will give you some information about the company, the form I am analysing and " \
             f"then a text section of that form. All of this delimited by ^^^. " \
             f"Summarize the section keeping it as short as possible, without leaving out " \
             f"any information that could be relevant to an investor in the company. " \
             f"If there is any reference to debt issuance write the interest rate, if present." \
             f"Organize the output in a list of short information points (around 20 words each)." \
             f"Remove all the points that contain duplicate information." \
             f"Do not refer to exhibits." \
             f"Format the output as a json with a single key 'data' and value as a list of the information points." \
             f"^^^" \
             f"Company Name: {company_name}" \
             f"Ticker: {ticker}" \
             f"Exchange: {exchange}" \
             f"Form: {form}" \
             f"Filing date: {filing_date}" \
             f"Section title: {section_title}" \
             f"Section text: {section_text}" \
             f"^^^"

    messages = [
        INITIAL_CONTEXT_MESSAGE,
        {"role": "user", "content": prompt},
    ]

    return messages


def create_summary(section_text, model, chain_type="map_reduce", verbose=False):
    llm = ChatOpenAI(model_name=model, openai_api_key=parser.get("open_ai", "api_key"))
    string_loader = UnstructuredStringLoader(section_text)
    docs = split_doc_in_chunks(string_loader.load())
    # chain = load_summarize_chain(llm, chain_type="refine", verbose=verbose)
    chain = load_summarize_chain(llm, chain_type=chain_type, verbose=verbose)

    with get_openai_callback() as cb:
        res = chain.run(docs)

    return res, cb.total_tokens


def check_input_tokens(input_tokens, model):
    # TODO decide an appropriate way to check input_tokens length
    return input_tokens > MODEL_MAX_TOKENS[model] - 500


def summarize_section(section_text, model="gpt-3.5-turbo", chain_type="map_reduce", verbose=False):
    """
    Create a summary for a document section.
    Output is a json {"data":["info1", "info2", ..., "infoN"]}
    :param company: company information (name, ticker, exchange), to give additional context to the model
    :param form: form type, to give additional context to the model
    :param filing_date: to give additional context to the model
    :param section_title: to give additional context to the model
    :param section_text: text input for the model
    :return: summary in json with a list of brief information points
    """

    summary, tokens = create_summary(section_text, model, chain_type, verbose)

    bullets = [x.strip() for x in summary.split(". ")]
    cost = compute_cost(tokens, model=model)

    return bullets, cost