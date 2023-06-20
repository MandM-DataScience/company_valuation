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
    # remove parentesis and strip
    title = re.sub(r'\([^)]*\)', '', title).strip(string.punctuation + string.whitespace)

    return title


def is_title_valid(text):
    valid = not (
            text.startswith("item") or
            text.startswith("part") or
            text.startswith("signature") or
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
    return chosen_table


def get_sections_using_hrefs(soup, chosen_table):
    """
    Scan the chosen_table aka TABLE of CONTENTS and identify all hrefs.
    With this, the method create a dictionary of sections by finding tag elements referenced inside soup with the specific hrefs
    :param soup:
    :param chosen_table:
    :return: section dictionary
    """
    all_elements = soup.find_all()
    hrefs = {}
    sections = {}
    for tr in chosen_table.findAll("tr"):
        try:
            aa = tr.find_all("a")
            tr_hrefs = [a['href'][1:] for a in aa]
        except:
            continue

        for el in tr.children:
            text = el.text
            text = clean_section_title(text)

            if is_title_valid(text):
                for tr_href in tr_hrefs:
                    if tr_href not in hrefs:
                        h_tag = soup.find(id=tr_href)
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

    return sections


def get_sections_using_strings(soup, chosen_table):
    print("GET SECTIONS USING STRINGS")
    sections = {}
    for tr in chosen_table.findAll("tr"):

        # tr_text = unidecode(tr.text.lower().strip(string.punctuation + string.whitespace).replace("\n", " "))
        # tr_text = re.sub(' +', ' ', tr_text)
        # while tr_text[-1].isdigit():
        #     tr_text = tr_text[:-1]
        # tr_text = tr_text.strip()
        # print(f"'{tr_text}'")
        # has_item = 'item' in tr_text
        # if has_item:
        #     reg_text = tr_text
        #     if '.' in tr_text:
        #         reg_text = reg_text.replace(".", "\.")
        #
        #     reg_text = reg_text.replace(" ", "*")
        #
        #     items_found = soup.find_all(string=re.compile(fr'{reg_text}', re.IGNORECASE))
        #     print(f"TEXT: {reg_text} --> {items_found}")
        #     input("next")

        for el in tr.children:
            text = unidecode(el.text.lower().strip(string.punctuation + string.whitespace))
            # if text starts with Item
            # print(f"'{text}' --> {'item' in text}")
            has_item = 'item' in text
            if has_item:
                reg_text = text
                if '.' in text:
                    reg_text = text
                items_found = soup.find_all(string=re.compile(fr'\b{reg_text}\b', re.IGNORECASE))
                print(f"TEXT: {text} --> {items_found}")
                # for item_found in items_found:
                if len(items_found) == 2:
                    print(text, items_found[1])
                input("next")

    return sections


def get_all_sections_v4(soup):
    """
    Retrieve sections in html text. A section has a title string and start tag element.
    :param soup:
    :return: a dictionary with the following structure:
        {1:
            {
                'start_el': tag element where the section starts,
                'idx': an integer index of start element inside soup, used for ordering
                'title': a string representing the section title,
                'title_candidates': a list of title candidates. If there is a single candidate that becomes the title
                'end_el': tag element where the section ends
            },
        ...
        }
        Section are ordered based on chid['idx'] value
    """

    chosen_table = identify_table_of_contents(soup)
    sections = {}

    if chosen_table is not None:
        sections = get_sections_using_hrefs(soup, chosen_table)

        if len(sections) == 0:
            # TODO try retrieve sections with table of contents using strings instead of hrefs
            sections = get_sections_using_strings(soup, chosen_table)

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


def parse_v4():
    from bs4 import BeautifulSoup, Tag

    import mongodb
    docs = mongodb.get_collection_documents("documents")
    count = mongodb.get_collection("documents").count_documents({})
    skip = True
    enable_print = False
    to_test = [
        "https://www.sec.gov/Archives/edgar/data/1019034/000143774923016374/bkyi20221231_10k.htm",
        "https://www.sec.gov/Archives/edgar/data/69733/000143774923016924/nath20230326_10k.htm",
        "https://www.sec.gov/Archives/edgar/data/88948/000143774923017258/senea20230331_10k.htm",
    ]
    _10k_no_sections = []
    _10k_no_text = []
    with_exception = []

    for i, doc in enumerate(docs):
        # print(f"{i}/{count}")
        url = doc["_id"]
        form_type = doc["form_type"]
        sections = {}
        try:
            cik = doc["cik"]
            filing_date = doc["filing_date"]
            html = doc["html"]

            if form_type not in ["10-K", "10-K/A"]:
                # print("continue because form type")
                continue

            if doc['_id'] in to_test:
                skip = False
                enable_print = True
            else:
                skip = True

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

            sections = get_all_sections_v4(soup)
            # print(sections)
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
                    found_text = unidecode(el.text)
                    if sections[current_section]['title'] is None:
                        if found_text in sections[current_section]['title_candidates']:
                            print(f"{bcolors.OKCYAN}"
                                  f'new title for {current_section}: {found_text} in {sections[current_section]["title_candidates"]}'
                                  f"{bcolors.ENDC}")
                            sections[current_section]['title'] = found_text
                    text += found_text

            if current_section is None:
                if "/A" in form_type:
                    print(f"{bcolors.OKGREEN}"
                          f'{url} - {form_type} with no SECTIONS'
                          f"{bcolors.ENDC}")
                else:
                    print(f"{bcolors.FAIL}"
                          f'{url} - {form_type} with NO SECTIONS'
                          f"{bcolors.ENDC}")
                    _10k_no_sections.append(url)
            else:
                sections[current_section]["text"] = text

            for s in sections:
                if 'text' not in sections[s]:
                    print(f"{bcolors.FAIL}"
                          f'{url} - {form_type} with NO TEXT'
                          f"{bcolors.ENDC}")
                    _10k_no_text.append(url)
        except Exception as e:
            print(f"{bcolors.FAIL}"
                  f'{url} - {form_type} with EXCEPTION'
                  f"{bcolors.ENDC}")
            with_exception.append((url, e))
            raise (e)

        # print results
        if enable_print:
            for s in sections:
                end_el = None
                if "end_el" in sections[s]:
                    end_el = sections[s]["end_el"]
                if "text" in sections[s]:
                    sc = ''
                    ec = ''
                    if sections[s]['title'] is None:
                        sc = f"{bcolors.OKBLUE}"
                        ec = f"{bcolors.ENDC}"
                    print(sc, sections[s]["idx"], s, sections[s]["title"], sections[s]["title_candidates"],
                          sections[s]["start_el"], end_el, len(sections[s]["text"]), ec)
                else:
                    print(f"{bcolors.WARNING}"
                          f' {sections[s]["idx"]} | {s} |{sections[s]["title"]} | {sections[s]["start_el"]} | {end_el} | has no TEXT'
                          f"{bcolors.ENDC}")
        print()
        input("NEXT")

    print("10K NO SECTIONS")
    for ns in _10k_no_sections:
        print(ns)

    print("10K NO TEXT")
    for ns in _10k_no_text:
        print(ns)

    print("WITH EXCEPTION")
    for ns in with_exception:
        print(ns)

    print("END")


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


if __name__ == '__main__':
    # test_parse_document()
    # parse_v2()
    # download_submissions_documents("0000764065")
    # parse_segments()
    # find_possible_axis()
    # parse_v3()
    parse_v4()
