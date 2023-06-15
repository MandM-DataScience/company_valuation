import json
import os
import time
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver

import mongodb
from edgar_utils import company_from_cik, AAPL_CIK, download_submissions_documents, download_all_cik_submissions


def parse_10_K(soup):

    section = None
    next_section = False
    result = {}
    body = soup.body

    # remove tables
    for table in body.find_all("table"):
        table.decompose()

    divs = body.findAll("div")
    for div in divs:

        span = div.find("span")

        # A new section is identified as:
        # <div> <span> Item .... Section name </span> </div> - new section is starting
        # <div> <span>  </span> </div> - section content
        # <div> <span> Item .... Section name </span> </div> - end previous section

        if span is not None:

            # if next_section and not span.text.startswith("Item"):
            #
            #     # information contained in the document is finished
            #     if section == "Exhibit and Financial Statement Schedules":
            #         break
            #
            #     result[section] = ""
            #     next_section = False
            #     continue

            if span.text.startswith("Item"):
                next_section = True
                section = span.text.split(".")[1].strip()
                print(span.text, " ==> ", section)

                continue

        # if section is not None:
        #     text = div.findAll(string=True, recursive=False)
        #     for t in text:
        #         if "SIGNATURE" in t.strip():
        #             section = None
        #             break
        #
        #         result[section] += t.strip()


    return result


def find_summary_table(soup):
    body = soup.body
    t = body.find("table")
    print(t)
    href_in_table = [a['href'] for a in t.findAll("a")]
    return href_in_table


def find_parent_tag(child, tag):
    parent_element = child.parent
    while parent_element is not None and parent_element.name != tag:
        parent_element = parent_element.parent
    return parent_element


list_items_strings = [
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


def get_outermost_tag(tag):
    outermost_parent = tag
    while outermost_parent.name != 'body':
        if outermost_parent.parent.name != 'body':
            outermost_parent = outermost_parent.parent
        else:
            break
    return outermost_parent


def get_first_tag(soup, hrefs):
    return get_outermost_tag(soup.find(id=hrefs[0]))


def get_section_name(soup, hrefs):
    pass


def test_parse_document():

    from bs4 import BeautifulSoup, Tag

    import mongodb
    import re
    docs = mongodb.get_collection_documents("documents")
    for doc in docs:
        html = doc["html"]
        # if doc['_id'] != "https://www.sec.gov/Archives/edgar/data/758743/000119312523156706/d357819d10k.htm":
        #     continue
        print(doc['_id'])
        # with open(f"{doc['cik']}.html", "w+", encoding="utf-8") as f:
        #     f.write(html)

        soup = BeautifulSoup(html, features="html.parser")
        # href_in_table = find_summary_table(soup)
        # print(doc["cik"], len(href_in_table))
        if soup.body is None:
            continue
        tables = soup.body.findAll("table")
        chosen_table = None
        max_table = 0
        for t in tables:
            count = 0
            for s in list_items_strings:
                r = t.find(string=re.compile(f'{s}', re.IGNORECASE))
                if r is not None:
                    count += 1
            if count > max_table:
                chosen_table = t
                max_table = count
        if chosen_table:
            hrefs = []
            for a in chosen_table.findAll("a"):
                href = a['href']
                if href not in hrefs:
                    hrefs.append(href)
            hrefs = [h[1:] for h in hrefs]

            print(f"HREFS: {hrefs}")
            # GET first tag
            first_tag = get_first_tag(soup, hrefs)
            outermost = get_outermost_tag(first_tag)
            is_outermost = outermost == first_tag
            if not is_outermost:
                first_tag = outermost

            use_outermost = False
            if len(first_tag.text.strip()) == 0:
                current_tag = get_outermost_tag(first_tag)
                use_outermost = True
                print(f"FOUND IN OUTERMOST")
            else:
                current_tag = first_tag
            # GET section name
            section = current_tag.text.strip()
            print(section)
            result = {}
            c = 0
            while current_tag is not None:
                if section not in result:
                    result[section] = ""
                else:
                    text = current_tag.text.strip()
                    if len(text) > 0:
                        result[section] += f"{text}\n"
                current_tag = current_tag.next_sibling

                # GET NEW SECTION
                print(current_tag)
                c +=1
                if c > 2000:
                    break
                if current_tag and isinstance(current_tag, Tag):
                    if current_tag.has_attr('id') and current_tag['id'] in hrefs:
                        section = current_tag.text.strip()
                        print(section)
                    if use_outermost:
                        children_with_id = current_tag.select('[id]')
                        for children in children_with_id:
                            if children['id'] in hrefs:
                                section = get_outermost_tag(children).text.strip()
                                print(f"section in chidlren {section}")
                                break


            with open(f"results/{doc['cik']}.json", "w+", encoding="utf-8") as f:
                f.write(json.dumps(result))

            # print(result)
        # break
    # print(result)


def get_all_sections(soup):

    import re
    tables = soup.body.findAll("table")
    chosen_table = None
    max_table = 0
    section_strings = []

    for t in tables:
        count = 0

        for s in list_items_strings:
            r = t.find(string=re.compile(f'{s}', re.IGNORECASE))
            if r is not None:
                count += 1

        if count > max_table:
            chosen_table = t
            max_table = count

    if chosen_table is not None:
        hrefs = []
        for a in chosen_table.findAll("a"):
            href = a['href']
            if href not in hrefs:
                hrefs.append(href)

        print(hrefs)

        hrefs = [h[1:] for h in hrefs]

        for h in hrefs:
            h_tag = soup.find(id=h)

            while len(h_tag.text.strip()) <= 0:
                h_tag = h_tag.parent

            section_strings.append(h_tag.text.strip())

    return section_strings


def parse_v2():

    from bs4 import BeautifulSoup, Tag

    import mongodb
    import re
    docs = mongodb.get_collection_documents("documents")

    for doc in docs:

        url = doc["_id"]
        cik = doc["cik"]
        form_type = doc["form_type"]
        filing_date = doc["filing_date"]
        html = doc["html"]

        # if doc['_id'] != "https://www.sec.gov/Archives/edgar/data/1672688/000162828023020162/absi-20221231.htm":
        #     continue

        if form_type != "10-K":
            continue

        company_info = company_from_cik(cik)
        print(company_info)
        print(form_type)
        print(url)

        # with open(f"{doc['cik']}.html", "w+", encoding="utf-8") as f:
        #     f.write(html)

        soup = BeautifulSoup(html, features="html.parser")

        # href_in_table = find_summary_table(soup)
        # print(doc["cik"], len(href_in_table))

        if soup.body is None:
            continue

        all_text = soup.body.text.strip()

        sections = get_all_sections(soup)


        # print(sections)

        min_index = 0

        for i in range(1, len(sections)+1):

            # print(f"FROM {sections[i-1]} TO {sections[i]}")
            start = all_text.find(sections[i-1], min_index)
            if i == len(sections):
                end = -1
            else:
                end = all_text.find(sections[i], start)

            min_index = end
            # print(f"{start} -> {end} [{len(all_text[start:end])}] \t {sections[i-1]}")

            # print(start, end)
            # print(all_text[start:end])
            # print()
            # print()
            # print()

        # input("GO ON!")
        return


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




def text_blocks():
    import pandas as pd
    doc = mongodb.get_document("financial_data", "0000764065") # clf cik

    for m in doc["facts"]["us-gaap"]:
        print(m.lower())
        # if "text" in m.lower():
        #     print(m)

    # try:
    #     data = doc["facts"][tax][measure]["units"][unit]
    # except:
    #     return None

    # df = pd.DataFrame(data)

if __name__ == '__main__':

    # test_parse_document()
    # parse_v2()
    # download_submissions_documents("0000764065")
    parse_segments()
    # find_possible_axis()