import mongodb
from edgar_utils import company_from_cik
from openai_interface import summarize_section


def sections_summary(url):
    """
    Summarize all sections of a document using openAI API.
    Upsert summary on mongodb (overwrite previous one, in case we make changes to openai_interface)
    :param url: url of the document, used as id on mongodb
    :return:
    """
    doc = mongodb.get_document("documents",url)
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

        # get summary from openAI model
        summary = summarize_section(company, doc["form_type"], doc["filing_date"], section_title, section_text)
        result[section_title] = summary

    mongodb.upsert_document("items_summary", result)