from configparser import ConfigParser

import openai
import os
import json
import tiktoken

parser = ConfigParser()
_ = parser.read(os.path.join("credentials.cfg"))
openai.api_key = parser.get("open_ai", "api_key")

INITIAL_CONTEXT_MESSAGE = {"role": "system", "content": "You are an assistant for investment analysis. Your goal is to help me make sense"
                                      " quickly of financial information available for US public companies on EDGAR."}



def get_completion(messages, model="gpt-3.5-turbo"):
    return openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0, # this is the degree of randomness of the model's output
    )

def tokenizer(text):
    enc = tiktoken.get_encoding("cl100k_base")
    return enc.encode(text)

def num_tokens_from_messages(messages, model="gpt-3.5-turbo"):
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")

    if model == "gpt-3.5-turbo":  # note: future models may deviate from this
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
    if model == "gpt-3.5-turbo":
        return round(tokens / 1000 * 0.002, 4)

def summarize_section(company, form, filing_date, section_title, section_text):

    company_name = company["name"]
    ticker = company["ticker"]
    exchange = company["exchange"]

    messages = [
        INITIAL_CONTEXT_MESSAGE,
        {"role": "user", "content": f"I will give you some information about the company, the form I am analysing and "
                                    f"then a section of that form. All of this delimited by ^^^. "
                                    f""
                                    f"Summarize the content keeping it as short as possible, without leaving out "
                                    f"any information that could be relevant to an investor in the company. "
                                    f"If there is any reference to debt issuance write the interest rate, if present."
                                    f"Organize the output in a list of short information points (around 20 words each)."
                                    f"Remove all the points that contain duplicate information."
                                    f"Do not refer to exhibits."
                                    f"Format the output as a json with a single key 'data' and value as a list of the information points."
                                    f""
                                    f"^^^"
                                    f"Company Name: {company_name}"
                                    f"Ticker: {ticker}"
                                    f"Exchange: {exchange}"
                                    f"Form: {form}"
                                    f"Filing date: {filing_date}"
                                    f"Section title: {section_title}"
                                    f"Section text: {section_text}"
                                    f"^^^"},
    ]

    input_tokens = num_tokens_from_messages(messages)

    response = get_completion(messages)

    usage = response["usage"]["total_tokens"]
    cost = compute_cost(usage)
    reply = response["choices"][0]["message"]["content"]

    # print(f"{usage} token ({input_tokens} as input), price {cost}$")
    # print()
    # print(reply)
    #
    # print()

    reply = json.loads(reply)
    reply["data"] = [x for x in reply["data"] if not "exhibit" in x.lower()]

    # print(reply)

    return reply
