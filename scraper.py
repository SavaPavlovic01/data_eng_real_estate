import requests
import json
from minio import Minio
from bs4 import BeautifulSoup
import io
from datetime import datetime, date

import gzip
from common_ops import send_to_s3, get_from_s3, decompress
from itertools import takewhile
import re


def scrape(page_num):
    resp = requests.get(f'https://www.nekretnine.rs/stambeni-objekti/stanovi/izdavanje-prodaja/prodaja/lista/po-stranici/10/stranica/{page_num}')
    soup = BeautifulSoup(resp.text, features="html.parser")

    offers = soup.find_all('p', {'class':'offer-price'})
    locations = soup.find_all('p', {'class':'offer-location text-truncate'})
    bodys = soup.find_all('div', {'class':'offer-body'})
    meta_info = soup.find_all('div', {'class':'offer-meta-info'})
    links = [link.h2.a['href'] for link in bodys]

    estate_info = []

    for i in range(0, len(offers) - 1, 2):
        meta_info_split = meta_info[i // 2].text.split("|")
        link_split = links[i // 2].split('/')
        estate_info.append(
            {
                'price':''.join([i for i in offers[i].span.text.strip().split() if i.isdigit()]),
                'size':int(''.join(takewhile(str.isdigit, offers[i + 1].span.text.strip()))),
                'location': locations[i // 2].text.strip(),
                'date_posted':meta_info_split[0].strip(),
                'rent_or_sale':meta_info_split[1].strip(),
                'rooms':meta_info_split[2].strip(),
                'id':link_split[len(link_split) - 2].strip(),
                'signature':hash(offers[i].span.text.strip() + link_split[len(link_split) - 2].strip()),
                'scraped_at':str(date.today())
            }
        )
    return estate_info

def scrape_n_pages(n = 100):
    all_data = []
    for i in range(n):
        all_data += scrape(i)  
        print(f'scraped {i}/{n}')
    return all_data

def scrape_and_save() -> str:
    return send_to_s3(scrape_n_pages(50), 'raw_data', compress=False)

from temp.save_local import save_local

if __name__ == "__main__":
    #scrape_and_save()
    save_local(json.dumps(scrape_n_pages(10)), 'raw_data')
    
    