from httpx import AsyncClient
from selectolax.parser import HTMLParser
from dataclasses import dataclass
from dotenv import load_dotenv
import os
import asyncio
import sqlite3
import duckdb
import re
import math
from urllib.parse import urljoin
import json
import pandas as pd

@dataclass
class HobbytronScraper:
    base_url: str = 'https://hobbytron.com'
    user_agent: str = 'Mozilla/5.0 (X11; Linux x86_64)'


    def extract_price(self, input_string):
        pattern = r'\d+\.\d{2}'
        match = re.search(pattern, input_string)
        if match:
            return match.group(0)  # Return the matched string

        return None

    def get_price(self, wholesaleprice):
        float_wholesaleprice = float(math.ceil(float(wholesaleprice)))
        if (wholesaleprice is None) or (wholesaleprice == 0):
            result = 0
        else:
            result = float_wholesaleprice - round(float_wholesaleprice * 5 / 100, 2)

        return result


    def get_compare_at_price(self, price):
        if (price is None) or (price == 0):
            result = 0
        else:
            result = round(float(math.ceil(price * 1.20)), 2)

        return result


    def extract_number(self, input_string):
        # pattern = r'#tab-(\d+)'
        pattern = r'(\d+)'
        match = re.search(pattern, input_string)
        if match:
            return match.group(1)

        return None


    async def fetch(self, aclient, url, limit):
        print(f'Fetching {url}...')
        headers = {
            'user-agent': self.user_agent,
        }

        async with limit:
            response = await aclient.get(url)
            if limit.locked():
                await asyncio.sleep(1)
            response.raise_for_status()

        print(f'Fetching {url}...Completed!')

        return url, response.text


    async def fetch_all(self, urls):
        tasks = []
        headers = {
            'user-agent': self.user_agent,
        }
        limit = asyncio.Semaphore(4)

        async with AsyncClient(headers=headers, timeout=120) as aclient:
            for url in urls:
                task = asyncio.create_task(self.fetch(aclient, url=url, limit=limit))
                tasks.append(task)

            htmls = await asyncio.gather(*tasks)

        return htmls


    def insert_to_db(self, htmls):
        if os.path.exists('hobbytron.db'):
            os.remove('hobbytron.db')
        conn = sqlite3.connect("hobbytron.db")
        curr = conn.cursor()
        curr.execute(
            """
            CREATE TABLE IF NOT EXISTS products_src(
            url TEXT,
            html BLOB
            )
            """
        )

        for html in htmls:
            curr.execute(
                "INSERT INTO products_src (url, html) VALUES(?,?)",
                html)
            conn.commit()


    def get_data(self):
        conn = sqlite3.connect("hobbytron.db")
        curr = conn.cursor()
        curr.execute("SELECT url, html FROM  products_src")
        datas = curr.fetchall()
        product_datas = list()
        for data in datas:
            current_product = {'Handle': '', 'Title': '', 'SKU': '', 'Description': '', 'Cost': 0.0, 'Price': 0.0,
                'ProductCategory': '', 'CompareAtPrice': 0.0, 'ImageSrc': '', 'ImageAltText': '', 'VideoSrc': '',
                'Tags': '', 'Available': False, 'Variants':''
            }

            tree = HTMLParser(data[1])
            product_elem = tree.css_first('div.card.card--collapsed.card--sticky')
            current_product['Handle'] = data[0].split('/')[-1]
            title_elem = product_elem.css_first('h1')

            if title_elem is not None:
                current_product['Title'] = title_elem.text(strip=True)

            sku_elem = product_elem.css_first('div.product-meta > div.product-meta__reference > span.product-meta__sku > span.product-meta__sku-number')
            if sku_elem is not None:
                current_product['SKU'] = sku_elem.text(strip=True)

            desc_elem = tree.css_first('div.product-block-list__item--description > div.card')
            if desc_elem is not None:
                current_product['Description'] = desc_elem.html

            cost_elem = tree.css_first('div.product-form__info-content > div.price-list > span.price.price--highlight')
            if cost_elem is not None:
                current_product['Cost'] = self.extract_price(cost_elem.text(strip=True))
            else:
                cost_elem = tree.css_first('div.product-form__info-content > div.price-list > span.price')
                if cost_elem is not None:
                    current_product['Cost'] = self.extract_price(cost_elem.text(strip=True))

            current_product['Price'] = self.get_price(current_product['Cost'])

            cat_elems = tree.css('li.breadcrumb__item')
            cat_list = list()
            for elem in cat_elems:
                cat_list.append(elem.text(strip=True))
            current_product['ProductCategory'] = ', '.join(cat_list)

            compare_at_price_elem = tree.css_first('div.product-form__info-content > div.price-list > span.price.price--compare')
            if compare_at_price_elem is not None:
                current_product['CompareAtPrice'] = self.extract_price(compare_at_price_elem.text(strip=True))

            image_elems = tree.css('a.product-gallery__thumbnail')
            image_src_list = list()
            image_alt_text_list = list()
            for elem in image_elems:
                image_src_list.append(urljoin(self.base_url, elem.attributes.get('href').split('?')[0]))
                image_alt_text_list.append(elem.css_first('img').attributes.get('alt'))
            current_product['ImageSrc'] = ', '.join(image_src_list)
            current_product['ImageAltText'] = ', '.join(image_alt_text_list)

            video_elem = desc_elem.css_first('div.rte.text--pull > p > iframe')
            if video_elem is not None:
                current_product['VideoSrc'] = video_elem.attributes.get('src')

            script_tags = tree.css('script[type="text/javascript"]')

            script_content = None

            for script in script_tags:
                if 'window.BoosterApps.common.product' in script.text():
                    script_content = script.text()
                    break

            if script_content:
                product_data_match = re.search(r'window\.BoosterApps\.common\.product\s*=\s*({.*?});', script_content, re.DOTALL)
                if product_data_match:
                    try:
                        product_data_str = product_data_match.group(1)
                        product_data_str = product_data_str.replace('id:', '"id":').replace('title:', '"title":')\
                            .replace('price:', '"price":').replace('handle:','"handle":' )\
                            .replace('tags:', '"tags":').replace('available:', '"available":')\
                            .replace('variants:', '"variants":')
                        product_data = json.loads(product_data_str)
                        current_product['Tags'] = ', '.join(product_data['tags'])
                        current_product['Available'] = product_data['available']
                        current_product['Variants'] = product_data['variants']

                        # variants = product_data['variants']
                        # current_variant = dict()
                        # list_variant = list()
                        # for variant in variants:
                        #     current_variant['RequiresShipping'] = variant['requires_shipping']
                        #     current_variant['Taxable'] = variant['taxable']
                        #     current_variant['Weight'] = variant['weight']
                        #     current_variant['InventoryManagement'] = variant['inventory_management']
                        #     current_variant['Barcode'] = variant['barcode']
                        #     current_variant['Option1Name'] = variant['option1']
                        #     current_variant['Option2Name'] = variant['option2']
                        #     current_variant['Option3Name'] = variant['option3']
                        #     list_variant.append(current_variant)
                        # current_product['Variants'] = list_variant

                    except json.JSONDecodeError as e:
                        print(f"Error parsing product data: {e}")
                        print(product_data_str)
                else:
                    print("Product data not found in the script.")
            else:
                print("Relevant script tag not found.")


            product_datas.append(current_product)

            # Using Pandas
            df = pd.DataFrame.from_records(product_datas)
            df = df.explode('Variants', ignore_index=True)
            print(df)
            df.to_csv('products_output.csv', index=False, sep=';')

            # Using Duck DB
            # con = duckdb.connect()

            # Create the table and insert data
            # con.execute("""CREATE TABLE products (Handle VARCHAR, Title VARCHAR, SKU VARCHAR, Description TEXT,
            #     Cost FLOAT, Price FLOAT, ProductCategory VARCHAR, CompareAtPrice FLOAT, ImageSrc TEXT,
            #     VideoSrc VARCHAR, Tags VARCHAR, Available BOOLEAN, RequiresShipping BOOLEAN, Taxable BOOLEAN,
            #     Weight FLOAT, InventoryManagement VARCHAR, Barcode VARCHAR)""")
            # for row in product_datas:
            #     con.execute("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row['Handle'], row['Title'], row['SKU'], row['Description'], row['Cost'], row['Price'], row['ProductCategory'], row['CompareAtPrice'], row['ImageSrc'], row['VideoSrc'], row['Tags'], row['Available'], row['RequiresShipping'], row['Taxable'], row['Weight'], row['InventoryManagement'], row['Barcode']))

            # Create the table and insert data
            # con.execute("""CREATE TABLE products (Handle VARCHAR, Title VARCHAR, SKU VARCHAR, Description TEXT,
            #     Cost FLOAT, Price FLOAT, ProductCategory VARCHAR, CompareAtPrice FLOAT, ImageSrc TEXT,
            #     VideoSrc VARCHAR, Tags VARCHAR, Available BOOLEAN, Variants TEXT)""")
            # for row in product_datas:
            #     con.execute("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            #         (row['Handle'], row['Title'], row['SKU'], row['Description'], row['Cost'],
            #             row['Price'], row['ProductCategory'], row['CompareAtPrice'], row['ImageSrc'],
            #             row['VideoSrc'], row['Tags'], row['Available'], row['Variants']
            #         )
            #     )

            # Export to CSV
            # con.execute("COPY products TO 'products_output.csv' (HEADER, DELIMITER ';')")
            # con.execute("""
            #     COPY(
            #         SELECT
            #             Handle,
            #             Title,
            #             SKU,
            #             Description,
            #             Cost,
            #             Price,
            #             ProductCategory,
            #             CompareAtPrice,
            #             ImageSrc,
            #             VideoSrc,
            #             Tags,
            #             Available,
            #             UNNEST(Variants)
            #         FROM
            #             products
            #     ) TO 'products_output.csv' WITH (HEADER, DELIMITER ';');
            # """)

            # con.close()


if __name__ == '__main__':
    urls = ['https://hobbytron.com/products/sonic-rc-helicopter', 'https://hobbytron.com/products/millennium-falcon-motion-sensing-quadcopter-by-world-tech-toys', 'https://hobbytron.com/products/luggage-playsets']

    hs = HobbytronScraper()
    # products_htmls = asyncio.run(hs.fetch_all(urls))
    # hs.insert_to_db(products_htmls)
    hs.get_data()