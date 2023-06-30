import copy
import json
import os
import re
import time
import traceback
from datetime import datetime

import Levenshtein as Levenshtein
from bs4 import BeautifulSoup, NavigableString
from unidecode import unidecode

import mongodb
from edgar_utils import company_from_cik, AAPL_CIK, download_submissions_documents, download_all_cik_submissions
import string

from openai_interface import summarize_section

list_10k_items = [
    "business",
    "risk factors",
    "unresolved staff comments",
    "properties",
    "legal proceedings",
    "mine safety disclosures",
    "market for registrant’s common equity, related stockholder matters and issuer purchases of equity securities",
    "reserved",
    "management’s discussion and analysis of financial condition and results of operations",
    "quantitative and qualitative disclosures about market risk",
    "financial statements and supplementary data",
    "changes in and disagreements with accountants on accounting and financial disclosure",
    "controls and procedures",
    "other information",
    "disclosure regarding foreign jurisdictions that prevent inspection",
    "directors, executive officers, and corporate governance",
    "executive compensation",
    "security ownership of certain beneficial owners and management and related stockholder matters",
    "certain relationships and related transactions, and director independence",
    "principal accountant fees and services",
    "exhibits and financial statement schedules",
]
default_10k_sections = {
     1: {'item': 'item 1', 'title': ['business']},
     2: {'item': 'item 1a', 'title': ['risk factor']},
     3: {'item': 'item 1b', 'title': ['unresolved staff']},
     4: {'item': 'item 2', 'title': ['propert']},
     5: {'item': 'item 3', 'title': ['legal proceeding']},
     6: {'item': 'item 4', 'title': ['mine safety disclosure', 'submission of matters to a vote of security holders']},
     7: {'item': 'item 5', 'title': ["market for registrant's common equity, related stockholder matters and issuer purchases of equity securities"]},
     8: {'item': 'item 6', 'title': ['reserved', 'selected financial data']},
     9: {'item': 'item 7', 'title': ["management's discussion and analysis of financial condition and results of operations"]},
     10: {'item': 'item 7a', 'title': ['quantitative and qualitative disclosures about market risk']},
     11: {'item': 'item 8', 'title': ['financial statements and supplementary data']},
     12: {'item': 'item 9', 'title': ['changes in and disagreements with accountants on accounting and financial disclosure']},
     13: {'item': 'item 9a', 'title': ['controls and procedures']},
     14: {'item': 'item 9b', 'title': ['other information']},
     15: {'item': 'item 9c', 'title': ['Disclosure Regarding Foreign Jurisdictions that Prevent Inspections']},
     16: {'item': 'item 10', 'title': ['directors, executive officers and corporate governance','directors and executive officers of the registrant']},
     17: {'item': 'item 11', 'title': ['executive compensation']},
     18: {'item': 'item 12', 'title': ['security ownership of certain beneficial owners and management and related stockholder matters']},
     19: {'item': 'item 13', 'title': ['certain relationships and related transactions']},
     20: {'item': 'item 14', 'title': ['principal accountant fees and services']},
     21: {'item': 'item 15', 'title': ['exhibits, financial statement schedules', 'exhibits and financial statement schedules']},
}
list_10q_items = [
    "financial statement",
    "risk factor",
    "legal proceeding",
    "mine safety disclosure",
    "management’s discussion and analysis of financial condition and results of operations",
    "quantitative and qualitative disclosures about market risk",
    "controls and procedures",
    "other information",
    "unregistered sales of equity securities and use of proceeds",
    "defaults upon senior securities",
    "exhibits"
]
default_10q_sections = {
    1: {'item': 'item 1', 'title': ['financial statement']},
    2: {'item': 'item 2', 'title': ["management's discussion and analysis of financial condition and results of operations"]},
    3: {'item': 'item 3', 'title': ['quantitative and qualitative disclosures about market risk']},
    4: {'item': 'item 4', 'title': ['controls and procedures']},
    5: {'item': 'item 1', 'title': ['legal proceeding']},
    6: {'item': 'item 1a', 'title': ['risk factor']},
    7: {'item': 'item 2', 'title': ["unregistered sales of equity securities and use of proceeds"]},
    8: {'item': 'item 3', 'title': ["defaults upon senior securities"]},
    9: {'item': 'item 4', 'title': ["mine safety disclosure"]},
    10: {'item': 'item 5', 'title': ["other information"]},
    11: {'item': 'item 6', 'title': ["exhibits"]},
}
default_8k_sections = {
    1: {'item': 'item 1.01', 'title': ["entry into a material definitive agreement"]},
    2: {'item': 'item 1.02', 'title': ["termination of a material definitive agreement"]},
    3: {'item': 'item 1.03', 'title': ["bankruptcy or receivership"]},
    4: {'item': 'item 1.04', 'title': ["mine safety"]},
    5: {'item': 'item 2.01', 'title': ["completion of acquisition or disposition of asset"]},
    6: {'item': 'item 2.02', 'title': ['results of operations and financial condition']},
    7: {'item': 'item 2.03', 'title': ["creation of a direct financial obligation"]},
    8: {'item': 'item 2.04', 'title': ["triggering events that accelerate or increase a direct financial obligation"]},
    9: {'item': 'item 2.05', 'title': ["costs associated with exit or disposal activities"]},
    10: {'item': 'item 2.06', 'title': ["material impairments"]},
    11: {'item': 'item 3.01', 'title': ["notice of delisting or failure to satisfy a continued listing"]},
    12: {'item': 'item 3.02', 'title': ["unregistered sales of equity securities"]},
    13: {'item': 'item 3.03', 'title': ["material modification to rights of security holders"]},
    14: {'item': 'item 4.01', 'title': ["changes in registrant's certifying accountant"]},
    15: {'item': 'item 4.02', 'title': ["non-reliance on previously issued financial statements"]},
    16: {'item': 'item 5.01', 'title': ["changes in control of registrant"]},
    17: {'item': 'item 5.02', 'title': ['departure of directors or certain officers']},
    18: {'item': 'item 5.03', 'title': ['amendments to articles of incorporation or bylaws']},
    19: {'item': 'item 5.04', 'title': ["temporary suspension of trading under registrant"]},
    20: {'item': 'item 5.05', 'title': ["amendment to registrant's code of ethics"]},
    21: {'item': 'item 5.06', 'title': ["change in shell company status"]},
    22: {'item': 'item 5.07', 'title': ['submission of matters to a vote of security holders']},
    23: {'item': 'item 5.08', 'title': ["shareholder director nominations"]},
    24: {'item': 'item 6.01', 'title': ["abs informational and computational material"]},
    25: {'item': 'item 6.02', 'title': ['change of servicer or trustee']},
    26: {'item': 'item 6.03', 'title': ['change in credit enhancement or other external support']},
    27: {'item': 'item 6.04', 'title': ["failure to make a required distribution"]},
    28: {'item': 'item 6.05', 'title': ["securities act updating disclosure"]},
    29: {'item': 'item 7.01', 'title': ["regulation fd disclosure"]},
    30: {'item': 'item 8.01', 'title': ['other events']},
    31: {'item': 'item 9.01', 'title': ["financial statements and exhibits"]},
}


def string_similarity_percentage(string1, string2):
    distance = Levenshtein.distance(string1.replace(" ", ""), string2.replace(" ", ""))
    max_length = max(len(string1), len(string2))
    similarity_percentage = (1 - (distance / max_length)) * 100
    # print(f"{string1} --> SIM: {similarity_percentage}")
    return similarity_percentage


def clean_section_title(title):
    # lower case
    title = title.lower()
    # remove special html characters
    title = unidecode(title)
    # remove item
    title = title.replace("item ", "")
    # remove '1.' etc
    for idx in range(20, 0, -1):
        for let in ['', 'a', 'b', 'c']:
            title = title.replace(f"{idx}{let}.", "")
    for idx in range(10, 0, -1):
        title = title.replace(f"f-{idx}", "")
    # remove parentesis and strip
    title = re.sub(r'\([^)]*\)', '', title).strip(string.punctuation + string.whitespace)

    return title


def is_title_valid(text):
    valid = not (
            text.startswith("item") or
            text.startswith("part") or
            text.startswith("signature") or
            text.startswith("page") or
            text.isdigit() or
            len(text) <= 2)
    # print(f"\n ############################ '{text}' == {valid}")
    # print(f"start with item {text.startswith('item ')}")
    # print(f"start with part {text.startswith('part ')}")
    # print(f"is digit {text.isdigit()}")
    # print(f"is empty {len(text) <= 2}")
    return valid


def parse_segments():
    done_ciks = []

    docs = mongodb.get_collection_documents("documents")
    for doc in docs:

        if "aapl" not in doc["_id"]:
            continue

        if doc["form_type"] != "10-K":
            continue

        cik = doc["_id"].split("data/")[1].split("/")[0]

        if cik in done_ciks:
            continue
        else:
            done_ciks.append(cik)

        print(f"######## {doc['_id']} ##########\n")

        page = doc["html"]
        soap = BeautifulSoup(page, features="html.parser")

        ix_resources = soap.find("ix:resources")
        contexts = ix_resources.findAll("xbrli:context")

        axis = [
            "srt:ProductOrServiceAxis",
            "us-gaap:StatementBusinessSegmentsAxis",
            "srt:ConsolidationItemsAxis",
            "srt:StatementGeographicalAxis",
        ]

        for c in contexts:

            context_id = c["id"]
            s = c.find("xbrli:segment")

            if s is not None:

                members = s.find_all("xbrldi:explicitmember")
                if len(members) == 0:
                    continue

                include = True
                for m in members:
                    if m["dimension"] not in axis:
                        include = False
                        break
                if not include:
                    continue

                try:
                    period = c.find("xbrli:enddate").text
                except:
                    period = c.find("xbrli:instant").text
                period = datetime.strptime(period, "%Y-%m-%d").date()

                # dimension = "+".join([x["dimension"] for x in members])
                # value = "+".join([x.text for x in members])

                # if dimension not in result_dict:
                #     result_dict[dimension] = {}
                #
                # if value not in result_dict[dimension] or period > result_dict[dimension][value]["period"]:
                #         result_dict[dimension][value] = {"period":period,"id":context_id}

                element = soap.find("ix:nonfraction", attrs={"contextref": context_id})
                if element is None:
                    continue

                segment = {}
                for m in members:
                    segment[m["dimension"]] = m.text

                print(f"{period} - {segment} => {element.text} ({element['name']})")

        return


def find_possible_axis():
    axis = []

    docs = mongodb.get_collection_documents("documents")
    for doc in docs:

        page = doc["html"]
        soap = BeautifulSoup(page, features="html.parser")

        ix_resources = soap.find("ix:resources")

        if ix_resources is None:
            continue

        contexts = ix_resources.findAll("xbrli:context")

        for c in contexts:
            s = c.find("xbrli:segment")
            if s is not None:
                try:
                    ax = [x["dimension"] for x in s.children]
                    for a in ax:
                        if a not in axis:
                            print(a)
                            axis.append(a)
                except:
                    pass


def identify_table_of_contents(soup, list_items):

    if list_items is None:
        return None

    max_table = 0
    chosen_table = None
    tables = soup.body.findAll("table")
    for t in tables:
        count = 0

        for s in list_items:
            r = t.find(string=re.compile(f'{s}', re.IGNORECASE))
            if r is not None:
                count += 1

        if count > max_table:
            chosen_table = t
            max_table = count
    if max_table > 3:
        return chosen_table
    return None


def get_sections_using_hrefs(soup, table_of_contents):
    """
    Scan the chosen_table aka TABLE of CONTENTS and identify all hrefs.
    With this, the method create a dictionary of sections by finding tag elements referenced inside soup with the specific hrefs
    Retrieve sections in html text. A section has a title string and start tag element.
    :param soup:
    :return: a dictionary with the following structure:
        {1:
            {
                'start_el': tag element where the section starts,
                'idx': an integer index of start element inside soup, used for ordering
                'title': a string representing the section title,
                'title_candidates': a list of title candidates. If there is a single candidate that becomes the title
                'end_el': tag element where the section ends,
                'text': the text of the section
            },
        ...
        }
        Section are ordered based on chid['idx'] value
    :param soup:
    :return: section dictionary
    """
    # print("WRITE to text")
    # with open("text.txt", "w", encoding="utf-8") as f:
    #     f.write(soup.body.get_text(separator=' '))
    all_elements = soup.find_all()
    hrefs = {}
    sections = {}
    for tr in table_of_contents.findAll("tr"):

        try:
            aa = tr.find_all("a")
            tr_hrefs = [a['href'][1:] for a in aa]
        except Exception as e:
            continue

        for el in tr.children:
            text = el.text
            text = clean_section_title(text)
            if is_title_valid(text):
                for tr_href in tr_hrefs:
                    if tr_href not in hrefs:
                        h_tag = soup.find(id=tr_href)
                        if h_tag is None:
                            h_tag = soup.find(attrs={"name": tr_href})
                        if h_tag:
                            hrefs[tr_href] = {
                                'start_el': h_tag,
                                'idx': all_elements.index(h_tag),
                                'title': None,
                                'title_candidates': set([text])}
                    else:
                        hrefs[tr_href]['title_candidates'].add(text)
            else:
                continue

    for h in hrefs:
        hrefs[h]['title_candidates'] = list(hrefs[h]['title_candidates'])
        if len(hrefs[h]['title_candidates']) == 1:
            hrefs[h]['title'] = hrefs[h]['title_candidates'][0]
        else:
            hrefs[h]['title'] = "+++".join(hrefs[h]['title_candidates'])

    temp_s = sorted(hrefs.items(), key=lambda x: x[1]["idx"])
    for i, s in enumerate(temp_s):
        sections[i + 1] = s[1]
        if i > 0:
            sections[i]["end_el"] = sections[i + 1]["start_el"]

    sections = get_sections_text_with_hrefs(soup, sections)
    return sections


def select_best_match(string_to_match, matches, start_index):
    match = None

    if start_index == 0:
        del matches[0]

    if len(matches) == 1:
        match = matches[0]
        if matches[0].start() > start_index:
            match = matches[0]
    elif len(matches) > 1:
        max_similarity = -1
        for i, m in enumerate(matches):
            if m.start() > start_index:
                sim = string_similarity_percentage(string_to_match, m.group().lower().replace("\n", " "))
                if sim > max_similarity:
                    max_similarity = sim
                    match = m
    return match


def get_sections_using_strings(soup, table_of_contents, default_sections):
    """
        Scan the chosen_table aka TABLE of CONTENTS and identify all text.

        Retrieve sections strings in soup.body.text.
        :param soup:
        :return: a dictionary with the following structure:
            {1:
                {
                    'start_index': the start index of the section inside soup.body.text
                    'end_index': the start index of the section inside soup.body.text,
                    'title': a string representing the section title,
                    'end_el': tag element where the section ends
                },
            ...
            }
            Section are ordered based on chid['idx'] value
        :param soup:
        :return: section dictionary
        """

    body_text = unidecode(soup.body.get_text(separator=" "))
    body_text = re.sub('\n', ' ', body_text)
    body_text = re.sub(' +', ' ', body_text)

    sections = {}
    if table_of_contents:
        num_section = 1
        for tr in table_of_contents.findAll("tr"):
            section = {}
            for el in tr.children:
                text = el.text
                item = unidecode(text.lower()).replace("\n", " ").strip(string.punctuation + string.whitespace)
                # print(text)
                # input("NEXt")
                # remove special html characters
                item = item
                if 'item' in item:
                    section["item"] = item

                text = clean_section_title(text)
                if 'item' in section and is_title_valid(text):
                    section['title'] = text
                    sections[num_section] = section
                    num_section += 1

    if len(sections) == 0:
        print(f"{bcolors.OKCYAN}"
              f'NO TABLE OF CONTENTS USABLE'
              f"{bcolors.ENDC}")
        sections = copy.deepcopy(default_sections)
        start_index = 1

    else:
        print(f"{bcolors.OKCYAN}"
              f'TABLE OF CONTENTS WITHOUT HREFS'
              f"{bcolors.ENDC}")
        start_index = 0

    # with open("text.txt", "w", encoding="utf-8") as f:
    #     f.write(body_text)

    for si in sections:
        s = sections[si]
        # print(s)
        if 'item' in s:
            match = None
            if isinstance(s['title'], list):
                for t in s['title']:
                    matches = list(re.finditer(fr"{s['item']}. *{t}", body_text, re.IGNORECASE + re.DOTALL))
                    if matches:
                        match = select_best_match(f"{s['item']} {t}", matches, start_index)
                        break
            else:
                matches = list(re.finditer(fr"{s['item']}. *{s['title']}", body_text, re.IGNORECASE + re.DOTALL))
                if matches:
                    match = select_best_match(f"{s['item']} {s['title']}", matches, start_index)

            if match is None:
                matches = list(re.finditer(fr"{s['item']}", body_text, re.IGNORECASE + re.DOTALL))
                # print(matches)
                if matches:
                    match = select_best_match(f"{s['item']}", matches, start_index)

            if match:
                s['title'] = match.group()
                s["start_index"] = match.start()
                start_index = match.start()
                # print(s)
            else:
                print(f"{bcolors.FAIL}"
                      f"FAILED TO FIND MATCH for {s}"
                      f"{bcolors.ENDC}")
                s['remove'] = True
        # input("NEXT")

    sections_temp = {}
    for si in sections:
        if "remove" not in sections[si]:
            sections_temp[si] = sections[si]

    temp_s = sorted(sections_temp.items(), key=lambda x: x[1]["start_index"])
    sections = {}
    last_section = 0
    for i, s in enumerate(temp_s):
        sections[i + 1] = s[1]
        if i > 0:
            sections[i]["end_index"] = sections[i + 1]["start_index"]
            sections[i]["text"] = body_text[sections[i]["start_index"]:sections[i]["end_index"]]
        last_section = i + 1

    # GET section text
    if last_section > 0:
        sections[last_section]["end_index"] = -1
        sections[last_section]["text"] = body_text[sections[last_section]["start_index"]:sections[last_section]["end_index"]]

    return sections


def get_sections_text_with_hrefs(soup, sections):
    # for s in sections:
    #     print(sections[s]["title"])
    next_section = 1
    current_section = None
    text = ""
    last_was_new_line = False
    for el in soup.body.descendants:
        if next_section in sections and el == sections[next_section]['start_el']:
            if current_section is not None:
                # print(f"END {current_section} | {sections[current_section]['title']}")
                sections[current_section]["text"] = text
                text = ""
                last_was_new_line = False
                # input("NEXT SECTION")

            current_section = next_section
            next_section += 1
            # print(f"START {current_section} | {sections[current_section]['title']}")

        if current_section is not None and isinstance(el, NavigableString):
            if last_was_new_line and el.text == "\n":
                continue
            elif el.text == "\n":
                last_was_new_line = True
            else:
                last_was_new_line = False
            found_text = unidecode(el.get_text(separator=" "))
            if sections[current_section]['title'] is None:
                if found_text in sections[current_section]['title_candidates']:
                    print(f"{bcolors.OKCYAN}"
                          f'new title for {current_section}: {found_text} in {sections[current_section]["title_candidates"]}'
                          f"{bcolors.ENDC}")
                    sections[current_section]['title'] = found_text
            if len(text) > 0 and text[-1] != " " and len(found_text) > 0 and found_text[0] != " ":
                text += "\n"
            text += found_text.replace('\n', ' ')

    if current_section is not None:
        sections[current_section]["text"] = text

    return sections


def parse_document(doc, form):

    if form == "10-K":
        include_forms = ["10-K", "10-K/A"]
        list_items = list_10k_items
        default_sections = default_10k_sections
    elif form == "10-Q":
        include_forms = ["10-Q"]
        list_items = list_10q_items
        default_sections = default_10q_sections
    elif form == "8-K":
        include_forms = ["8-K"]
        list_items = None
        default_sections = default_8k_sections
    else:
        print(f"return because form_type {form} is not valid")
        return

    url = doc["_id"]
    form_type = doc["form_type"]
    filing_date = doc["filing_date"]
    sections = {}
    cik = doc["cik"]
    html = doc["html"]

    if form_type not in include_forms:
        print(f"return because form_type != {form}")
        return

    company_info = company_from_cik(cik)

    # no cik in cik_map
    if company_info is None:
        print("return because company info None")
        return

    print(f"form type: \t\t{form_type}")
    print(company_info)

    soup = BeautifulSoup(html, features="html.parser")

    if soup.body is None:
        print("return because soup.body None")
        return

    table_of_contents = identify_table_of_contents(soup, list_items)

    if table_of_contents:
        sections = get_sections_using_hrefs(soup, table_of_contents)

    if len(sections) == 0:
        sections = get_sections_using_strings(soup, table_of_contents, default_sections)

    for s in sections:
        if 'text' not in sections[s]:
            print(f"{bcolors.FAIL}"
                  f'{url} - {form_type} with NO TEXT'
                  f"{bcolors.ENDC}")

    result = {"_id": url, "cik": cik, "form_type":form_type, "filing_date": filing_date, "sections":{}}

    for s in sections:
        section = sections[s]
        if 'text' in section:
            text = section['text']
            text = re.sub('\n', ' ', text)
            text = re.sub(' +', ' ', text)
            result["sections"][section["title"]] = text

    try:
        mongodb.upsert_document("parsed_documents", result)
    except:
        traceback.print_exc()
        print(result.keys())
        print(result["sections"].keys())


    # business_section = find_business_section(sections)


def find_auditor(doc):

    soup = BeautifulSoup(doc["html"], features="html.parser")

    # auditor_start_string = 'Report of Independent Registered Public Accounting Firm'.lower()

    # auditor_string = ""
    body = unidecode(soup.body.get_text(separator=" "))
    body = re.sub('\n', ' ', body)
    body = re.sub(' +', ' ', body)

    start_sig = 0
    while start_sig != -1:
        start_sig = body.find('s/', start_sig+1)
        auditor_candidate = body[start_sig: start_sig+200]

        # print(auditor_candidate)
        if 'auditor since' in auditor_candidate.lower():
            pattern = r"s/.+auditor since.*?\d{4}"

            try:
                match = re.findall(pattern, auditor_candidate)[0]
                return match.replace("s/", "").strip()
            except:
                pass

    # print(auditor_string)

    # if auditor_start_string in body.lower():
    #     start_sig = 0
    #     while start_sig != -1:
    #         start_sig = body.lower().find(auditor_start_string, start_sig)
    #         if start_sig != -1:
    #             start_sig = body.find('s/', start_sig)
    #             end_sig = body.find('.', start_sig)
    #             auditor_candidate = body[start_sig: end_sig]
    #             if 'auditor since' in auditor_candidate.lower():
    #                 auditor_string += body[start_sig: end_sig] + "\n"
    # if auditor_string == "":
    #     auditor_start_string = 'auditor'
    #     if auditor_start_string in body.lower():
    #         start_sig = 0
    #         while start_sig != -1:
    #             start_sig = body.lower().find(auditor_start_string, start_sig + len(auditor_start_string))
    #             if start_sig != -1:
    #                 auditor_string += body[start_sig - 100: start_sig + 100] + "\n"
    #                 print(auditor_string)

    # return auditor_string


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def test():
    test_docs = {
        "docs_with_table_of_contents_and_hrefs": [
            "https://www.sec.gov/Archives/edgar/data/12040/000117494723000017/form10k-29127_bdl.htm",
        ],
        "docs_with_table_of_contents_no_hrefs": [
            "https://www.sec.gov/Archives/edgar/data/10329/000143774923001642/bset20230109_10k.htm",
        ],
        "docs_without_table_of_contents": [
            "https://www.sec.gov/Archives/edgar/data/315374/000155837023000097/hurc-20221031x10k.htm",
            "https://www.sec.gov/Archives/edgar/data/97476/000009747623000007/txn-20221231.htm",
            "https://www.sec.gov/Archives/edgar/data/315213/000031521323000016/rhi-20221231.htm" # item from 10 to 14 are missing in filing
        ]
    }
