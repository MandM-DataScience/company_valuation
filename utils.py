import copy
import json
import os
import re
import time
from datetime import datetime

import Levenshtein as Levenshtein
from bs4 import BeautifulSoup, NavigableString
from unidecode import unidecode

import mongodb
from edgar_utils import company_from_cik, AAPL_CIK, download_submissions_documents, download_all_cik_submissions
import string


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
default_sections = {
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
     16: {'item': 'item 9c', 'title': ['Disclosure Regarding Foreign Jurisdictions that Prevent Inspections']},
     16: {'item': 'item 10', 'title': ['directors, executive officers and corporate governance','directors and executive officers of the registrant']},
     17: {'item': 'item 11', 'title': ['executive compensation']},
     18: {'item': 'item 12', 'title': ['security ownership of certain beneficial owners and management and related stockholder matters']},
     19: {'item': 'item 13', 'title': ['certain relationships and related transactions']},
     20: {'item': 'item 14', 'title': ['principal accountant fees and services']},
     21: {'item': 'item 15', 'title': ['exhibits, financial statement schedules', 'exhibits and financial statement schedules']},
}

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
                c += 1
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


def get_all_sections(soup, THRESHOLD=50):
    import re
    tables = soup.body.findAll("table")
    chosen_table = None
    max_table = 0
    num_section = 1
    sections = {}

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
        for tr in chosen_table.findAll("tr"):

            try:
                a = tr.find_all("a")[0]
            except:
                continue
            href = a['href'][1:]
            for el in tr.children:
                text = el.text
                text = clean_section_title(text)

                if is_title_valid(text):
                    sections[num_section] = {"title": text, "href": href}
                    num_section += 1
                else:
                    continue

        # print(sections)

        for s in sections:
            h = sections[s]["href"]
            h_tag = soup.find(id=h)
            if h_tag is None:
                h_tag = soup.find_all(attrs={"name": h})[0]

            # print(sections[s], "=>", h_tag)

            while h_tag.parent.name != "body":
                h_tag = h_tag.parent

            # print(h, hrefs[h], "=>", h_tag)
            found = False
            while not found:
                h_tag_text = unidecode(h_tag.text.lower())
                similarity = string_similarity_percentage(sections[s]["title"], h_tag_text)

                if sections[s]["title"] in h_tag_text or similarity > THRESHOLD:
                    found = True
                    sections[s]["start_el"] = h_tag
                    # print(f"FOUND ({sections[s]['title']}) in {h_tag.text} ({similarity})")
                # else:
                # print(f"not found ({sections[s]['title']}) in {h_tag.text} ({similarity})")

                h_tag = h_tag.next_sibling

            # print(sections[s], "=>", h_tag)

        all_elements = soup.find_all()

        for k in sections:
            idx = all_elements.index(sections[k]["start_el"])
            sections[k]["idx"] = idx
            # print(sections[k]["title"], "IDX=", idx)

        sections = dict(sorted(sections.items(), key=lambda x: x[1]["idx"]))

        keys = list(sections.keys())
        for i, k in enumerate(keys):
            if i < len(keys) - 1:
                sections[k]["end_el"] = sections[keys[i + 1]]["start_el"]

    return sections


def parse_v2():
    from bs4 import BeautifulSoup, Tag

    import mongodb
    docs = mongodb.get_collection_documents("documents")
    skip = False

    for doc in docs:

        url = doc["_id"]
        cik = doc["cik"]
        form_type = doc["form_type"]
        filing_date = doc["filing_date"]
        html = doc["html"]

        if form_type != "10-K":
            # print("continue because form type")
            continue

        if doc['_id'] == "https://www.sec.gov/Archives/edgar/data/108385/000010838523000022/wrld-20230331.htm":
            skip = False

        if skip:
            # print("continue because skip")
            continue

        input("NEXT")
        print(url)
        company_info = company_from_cik(cik)

        # no cik in cik_map
        if company_info is None:
            print("continue because company info None")
            continue

        print(f"form type: \t\t{form_type}")
        print(company_info)

        # with open(f"{doc['cik']}.html", "w+", encoding="utf-8") as f:
        #     f.write(html)

        soup = BeautifulSoup(html, features="html.parser")
        # href_in_table = find_summary_table(soup)
        # print(doc["cik"], len(href_in_table))

        if soup.body is None:
            print("continue because soup.body None")
            continue

        # all_text = soup.body.text.strip()

        sections = get_all_sections(soup)
        print(sections)

        for s in sections:

            text = ""
            el = sections[s]["start_el"]

            if "end_el" in sections[s]:
                while el != sections[s]["end_el"]:
                    text += el.text
                    el = el.next_sibling
            else:
                while el.next_sibling is not None:
                    text += el.text
                    el = el.next_sibling

            sections[s]["text"] = unidecode(text)

            print(sections[s]["title"], len(sections[s]["text"]))

        print()
    print("END")


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


def get_all_sections_v3(soup):
    """
    Retrieve sections in html text. A section has a title string and start tag element.
    :param soup:
    :return:
    """
    import re
    tables = soup.body.findAll("table")
    chosen_table = None
    max_table = 0
    num_section = 1
    sections = {}

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
        for tr in chosen_table.findAll("tr"):
            try:
                a = tr.find_all("a")[0]
            except:
                continue
            href = a['href'][1:]
            for el in tr.children:
                text = el.text
                text = clean_section_title(text)

                if is_title_valid(text):
                    sections[num_section] = {"title": text, "href": href}
                    num_section += 1
                else:
                    continue

        # print(sections)

        for s in sections:
            h = sections[s]["href"]
            h_tag = soup.find(id=h)

            if h_tag is None:
                h_tag = soup.find_all(attrs={"name": h})[0]

            sections[s]["start_el"] = h_tag
            sections[s]["idx"] = h_tag.sourceline + h_tag.sourcepos

        sections = dict(sorted(sections.items(), key=lambda x: x[1]["idx"]))

        keys = list(sections.keys())
        for i, k in enumerate(keys):
            if i < len(keys) - 1:
                sections[k]["end_el"] = sections[keys[i + 1]]["start_el"]
    return sections


def identify_table_of_contents(soup):
    max_table = 0
    chosen_table = None
    tables = soup.body.findAll("table")
    for t in tables:
        count = 0

        for s in list_items_strings:
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


def get_sections_using_strings(soup, table_of_contents):
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

def parse_v3():
    from bs4 import BeautifulSoup, Tag

    import mongodb
    docs = mongodb.get_collection_documents("documents")
    skip = True

    for doc in docs:

        url = doc["_id"]
        cik = doc["cik"]
        form_type = doc["form_type"]
        filing_date = doc["filing_date"]
        html = doc["html"]

        if form_type != "10-K":
            # print("continue because form type")
            continue

        if doc['_id'] == "https://www.sec.gov/Archives/edgar/data/1302215/000130221523000031/hli-20230331.htm":
            skip = False

        if skip:
            # print("continue because skip")
            continue

        print(url)
        company_info = company_from_cik(cik)

        # no cik in cik_map
        if company_info is None:
            print("continue because company info None")
            continue

        print(f"form type: \t\t{form_type}")
        print(company_info)

        # with open(f"{doc['cik']}.html", "w+", encoding="utf-8") as f:
        #     f.write(html)

        soup = BeautifulSoup(html, features="html.parser")
        # href_in_table = find_summary_table(soup)
        # print(doc["cik"], len(href_in_table))

        if soup.body is None:
            print("continue because soup.body None")
            continue

        sections = get_all_sections_v3(soup)

        # print(sections)
        next_section = 1
        current_section = None
        text = ""
        last_was_new_line = False
        for el in soup.body.descendants:
            if next_section in sections and el == sections[next_section]['start_el']:
                if current_section is not None:
                    # print(f"END {sections[current_section]['title']}")
                    sections[current_section]["text"] = text
                    text = ""
                    last_was_new_line = False
                    # input("NEXT SECTION")

                current_section = next_section
                next_section += 1
                # print(f"START {sections[current_section]['title']}")

            if current_section is not None and isinstance(el, NavigableString):
                if last_was_new_line and el.text == "\n":
                    continue
                elif el.text == "\n":
                    last_was_new_line = True
                else:
                    last_was_new_line = False
                text += unidecode(el.text)

        sections[current_section]["text"] = text
        for s in sections:
            if "text" in sections[s]:
                print(sections[s]["title"], sections[s]["start_el"], sections[s]["end_el"], len(sections[s]["text"]))
            else:
                end_el = None
                if "end_el" in sections[s]:
                    end_el = sections[s]["end_el"]
                print(f'\n{sections[s]["title"]} | {sections[s]["start_el"]} | {end_el} | has no TEXT')
        print()
        input("NEXT")

    print("END")


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


def parse_v4():
    from bs4 import BeautifulSoup, Tag

    import mongodb
    mdb_query = {"filing_date": {"$gt": "2023-02-20", "$lte": "2023-02-25"}}
    docs = mongodb.get_collection("documents").find(mdb_query)
    count = mongodb.get_collection("documents").count_documents(mdb_query)
    enable_print = False
    skip = False
    only_test = False
    ask_next = True
    to_test = [
        "https://www.sec.gov/Archives/edgar/data/61398/000006139823000011/tell-20221231.htm",
    ]
    _10k_no_sections = []
    _10k_no_text = []
    with_exception = []
    _10k_no_toc = []

    for i, doc in enumerate(docs):
        print(f"{i}/{count}")
        url = doc["_id"]
        form_type = doc["form_type"]
        sections = {}
        cik = doc["cik"]
        filing_date = doc["filing_date"]
        html = doc["html"]

        if form_type not in ["10-K", "10-K/A"]:
            # print("continue because form type")
            continue

        if len(to_test) > 0 and doc['_id'] in to_test:
            skip = False
            enable_print = True
        elif only_test:
            skip = True
        # else:
        #     skip = True

        if skip:
            # print("continue because skip")
            continue

        print(url)
        company_info = company_from_cik(cik)

        # no cik in cik_map
        if company_info is None:
            print("continue because company info None")
            continue

        print(f"form type: \t\t{form_type}")
        print(company_info)

        soup = BeautifulSoup(html, features="html.parser")

        if soup.body is None:
            print("continue because soup.body None")
            continue

        table_of_contents = identify_table_of_contents(soup)

        if table_of_contents is None:
            if "/A" in form_type:
                print(f"{bcolors.OKGREEN}"
                      f'{url} - {form_type} with NO TABLE OF CONTENTS'
                      f"{bcolors.ENDC}")
                _10k_no_toc.append(url)
            else:
                print(f"{bcolors.WARNING}"
                      f'{url} - {form_type} with NO TABLE OF CONTENTS'
                      f"{bcolors.ENDC}")
                _10k_no_toc.append(url)
            # continue
            # input("NEXT")

        if table_of_contents:
            print("HAS TABLE OF CONTENTS")
            sections = get_sections_using_hrefs(soup, table_of_contents)

        if len(sections) == 0:
            sections = get_sections_using_strings(soup, table_of_contents)
        # else:
        #     print(f"{bcolors.OKCYAN}"
        #           f'TABLE OF CONTENTS AND HREFS'
        #           f"{bcolors.ENDC}")
        #
        # if len(sections) == 0:
        #     print(f"{bcolors.FAIL}"
        #           f'{url} - {form_type} with NO SECTIONS'
        #           f"{bcolors.ENDC}")
        #     _10k_no_sections.append(url)

        for s in sections:
            if 'text' not in sections[s]:
                print(f"{bcolors.FAIL}"
                      f'{url} - {form_type} with NO TEXT'
                      f"{bcolors.ENDC}")
                _10k_no_text.append(url)

        # print results
        # if enable_print:
        #     for s in sections:
        #         end_el = None
        #         if "end_el" in sections[s]:
        #             end_el = sections[s]["end_el"]
        #         if "text" in sections[s]:
        #             sc = ''
        #             ec = ''
        #             if sections[s]['title'] is None:
        #                 sc = f"{bcolors.OKBLUE}"
        #                 ec = f"{bcolors.ENDC}"
        #             print(sc, sections[s]["idx"], s, sections[s]["title"], sections[s]["title_candidates"],
        #                   sections[s]["start_el"], end_el, len(sections[s]["text"]), ec)
        #         else:
        #             print(f"{bcolors.WARNING}"
        #                   f' {sections[s]["idx"]} | {s} |{sections[s]["title"]} | {sections[s]["start_el"]} | {end_el} | has no TEXT'
        #                   f"{bcolors.ENDC}")

        business_section = find_business_section(sections)
        if business_section is not None:
            if 'text' in business_section:
                business_text = business_section['text']
                business_text = re.sub('\n', ' ', business_text)
                business_text = re.sub(' +', ' ', business_text)
                result = {
                    '_id': url,
                    'business': business_text,
                }
                mongodb.upsert_document("parsed_documents", result)

        # find_auditor(soup, sections)
        if ask_next:
            input("NEXT")

    # print("10K NO TABLE OF CONTENTS")
    # for ns in _10k_no_toc:
    #     print(ns)
    #
    # print("10K NO SECTIONS")
    # for ns in _10k_no_sections:
    #     print(ns)
    #
    # print("10K NO TEXT")
    # for ns in _10k_no_text:
    #     print(ns)
    #
    # print("WITH EXCEPTION")
    # for ns in with_exception:
    #     print(ns)

    print("END")


def find_business_section(sections):
    has_business = None
    for s in sections:
        sec = sections[s]

        if 'title' in sec and sec['title'] is not None and 'business' in sec['title'].lower():
            has_business = sec

        if has_business is None and 'title_candidates' in sec:
            for tc in sec['title_candidates']:
                if 'business' in tc.lower():
                    has_business = sec
                    break
        if has_business is None and 'item' in sec and 'item 1' == sec['item']:
            has_business = sec
    if has_business is None:
        print(f"{bcolors.FAIL}"
              f'NO BUSINESS'
              f"{bcolors.ENDC}")

        for s in sections:
            sec = sections[s]
            del sec['text']
            print(sec)

    return has_business


def find_auditor(soup, sections):
    auditor_start_string = 'Report of Independent Registered Public Accounting Firm'.lower()
    auditor_string = ""
    body = unidecode(soup.body.get_text(separator=" "))
    body = re.sub('\n', ' ', body)
    body = re.sub(' +', ' ', body)

    for s in sections:
        sec = sections[s]
        if auditor_start_string in sec['text'].lower():
            start_sig = 0
            while start_sig != -1:
                start_sig = sec['text'].lower().find(auditor_start_string, start_sig)
                if start_sig != -1:
                    start_sig = sec['text'].find('s/', start_sig)
                    end_sig = sec['text'].find('.', start_sig)
                    auditor_candidate = sec['text'][start_sig: end_sig]
                    if 'auditor' in auditor_candidate.lower():
                        auditor_string += sec['text'][start_sig: end_sig] + "\n"

    auditor_start_string = 'auditor'
    if auditor_string == "":
        if auditor_start_string in body.lower():
            start_sig = 0
            while start_sig != -1:
                start_sig = body.lower().find(auditor_start_string, start_sig + len(auditor_start_string))
                if start_sig != -1:
                    auditor_string += body[start_sig - 100: start_sig + 100] + "\n"

    print(auditor_string)


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


def evaluate_sections_summary(url):
    from openai_interface import evaluate_section_length, summarize_section

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

        section_text = "Avery Dennison Corporation is a global materials science and digital identification solutions company that provides branding and information labeling solutions. They serve an array of industries worldwide and have nearly 200 manufacturing and distribution facilities in over 50 countries. They have two reportable segments, Materials Group and Solutions Group, and have made acquisitions and venture investments to support organic growth. They have a global workforce of over 36,000 people and have established DEI initiatives, compensation and benefit programs, and hybrid and remote work opportunities. They prioritize safety and have a global Recordable Incident Rate of 0.23 in 2022, significantly lower than the Occupational Safety and Health Administration manufacturing industry average of 3.3 in 2021. They conduct a global employee engagement survey annually and strive to foster a collaborative, supportive culture to promote retention and minimize employee turnover. To ensure worker safety, they invest in solvent capture and control units and use adhesives and adhesive processing systems that minimize the use of solvents. They also provide security and digital tools to support efficiency and effectiveness for their employees."
        # get summary from openAI model
        summary = summarize_section(company, doc["form_type"], doc["filing_date"], section_title, section_text)
        result[section_title] = summary
    mongodb.upsert_document("items_summary", result)


if __name__ == '__main__':
    # test_parse_document()
    # parse_v2()
    # download_submissions_documents("0000764065")
    # parse_segments()
    # find_possible_axis()
    # parse_v3()
    # parse_v4()
    url = 'https://www.sec.gov/Archives/edgar/data/8818/000000881823000002/avy-20221231.htm'
    evaluate_sections_summary(url)