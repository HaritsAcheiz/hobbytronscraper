from httpx import AsyncClient
from pandas.core.base import NoNewAttributesMixin
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
import ast

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


    def get_review(self, product_id):
        url = 'https://hobbytron.com/search?view=static-recently-viewed-products&type=product&q=id:6554003701863'


    def extract_variants(self, df):
        df['Option1 Value'] = df['Variants'].apply(lambda x: x['option1'])
        df['Option2 Value'] = df['Variants'].apply(lambda x: x['option2'])
        df['Option3 Value'] = df['Variants'].apply(lambda x: x['option3'])
        df['Variant SKU'] = df['Variants'].apply(lambda x: x['sku'])
        df['Variant Grams'] = df['Variants'].apply(lambda x: round(x['weight']/100, 2))
        df['Variant Inventory Qty'] = df['Variants'].apply(lambda x: 10 if x['available'] else 0)
        df['Cost per item'] = df['Variants'].apply(lambda x: round(x['price']/100, 2))
        df['Variant Price'] = df['Cost per item'].apply(self.get_price)
        df['Variant Compare At Price'] = df['Variants'].apply(lambda x: round(x['compare_at_price']/100, 2) if not pd.isna(x['compare_at_price']) else '')
        df['Variant Requires Shipping'] = df['Variants'].apply(lambda x: x['requires_shipping'])
        df['Variant Taxable'] = df['Variants'].apply(lambda x: x['taxable'])
        df['Image Position'] = df['Variants'].apply(lambda x: x['featured_image']['position'] if not pd.isna(x['featured_image']) else '')
        df['Image Alt Text'] = df['Variants'].apply(lambda x: x['title'] if not pd.isna(x['featured_image']) else '')
        df['Variant Image'] = df['Variants'].apply(lambda x: 'https:' + x['featured_image']['src'].split('?')[0] if not pd.isna(x['featured_image']) else '')

        return df


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
            current_product = {'Handle': '', 'Title': '', 'Body (HTML)': '', 'Vendor': '', 'Product Category': '',
                'Type':'', 'Tags': '', 'Published': True, 'Option1 Name': 'Title', 'Option1 Value': 'Default Title',
                'Option2 Name': '', 'Option2 Value': '', 'Option3 Name': '', 'Option3 Value': '',
                'Variant SKU': '', 'Variant Grams': 0.0, 'Variant Inventory Tracker': 'shopify',
                'Variant Inventory Qty': 0, 'Variant Inventory Policy': 'deny', 'Variant Fulfillment Service': 'manual',
                'Variant Price': 0.0, 'Variant Compare At Price': 0.0, 'Variant Requires Shipping': True,
                'Variant Taxable': True, 'Variant Barcode': '', 'Image Src': '', 'Image Position': '',
                'Image Alt Text': '', 'Gift Card': True, 'SEO Title': '', 'SEO Description': '',
                'Google Shopping / Google Product Category': '', 'Google Shopping / Gender': '', 'Google Shopping / Age Group': '',
                'Google Shopping / MPN': '', 'Google Shopping / Condition': 'New', 'Google Shopping / Custom Product': '',
                'Google Shopping / Custom Label 0': '', 'Google Shopping / Custom Label 1': '', 'Google Shopping / Custom Label 2': '',
                'Google Shopping / Custom Label 3': '', 'Google Shopping / Custom Label 4': '', 'Collection URL (product.metafields.custom.collection_url)': '',
                'enable_best_price (product.metafields.custom.enable_best_price)': True, 'Google: Custom Product (product.metafields.mm-google-shopping.custom_product)': '',
                'Product rating count (product.metafields.reviews.rating_count)': '', 'Variant Image': '',
                'Variant Weight Unit': 'lb', 'Variant Tax Code': '', 'Cost per item': 0.0, 'Included / United States': True,
                'Price / United States': '', 'Included / United States': True, 'Price / United States': '', 'Compare At Price / United States': '',
                'Included / International': '', 'Price / International': '', 'Compare At Price / International': '', 'Status':'draft',
                'Video Src': '', 'Video Name': '', 'Variants': ''
            }

            tree = HTMLParser(data[1])

            product_elem = tree.css_first('div.card.card--collapsed.card--sticky')
            current_product['Handle'] = data[0].split('/')[-1]

            title_elem = product_elem.css_first('h1')
            if title_elem is not None:
                current_product['Title'] = title_elem.text(strip=True)

            desc_elem = tree.css_first('div.product-block-list__item--description > div.card')
            if desc_elem is not None:
                current_product['Body (HTML)'] = desc_elem.html

            current_product['Vendor'] = 'Hobbytron'

            cat_elems = tree.css('li.breadcrumb__item')
            cat_list = list()
            for elem in cat_elems:
                cat_list.append(elem.text(strip=True))
            current_product['Product Category'] = ' > '.join(cat_list)

            current_product['Type'] = ''

            battery_elem = tree.css_first('div.battry-upsell__content')
            if battery_elem is not None:
                current_product['Battery Option Value'] = battery_elem.css_first('p.battry_title').text(strip=True)
                current_product['Battery Price'] = float(self.extract_price(battery_elem.css_first('p.battry_price').text(strip=True)))

            option_elem = tree.css_first('span.product-form__option-name.text--strong')
            if (battery_elem is not None) & (option_elem is not None):
                current_product[f'Option1 Name'] = option_elem.text(strip=True).split(':')[0]
                current_product[f'Option2 Name'] = 'Battery'
            elif (battery_elem is not None) & (option_elem is None):
                current_product[f'Option2 Name'] = 'Battery'
            elif (battery_elem is None) & (option_elem is not None):
                current_product[f'Option1 Name'] = option_elem.text(strip=True).split(':')[0]

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
                        current_product['Variants'] = product_data['variants'].copy()
                        for variant in product_data['variants']:
                            if battery_elem is not None:
                                additional_option = {'id': variant['id'],
                                    'title': variant['title'],
                                    'option1': variant['option1'],
                                    'option2': current_product['Battery Option Value'],
                                    'option3': variant['option3'],
                                    'sku': f"{variant['sku']}-B",
                                    'requires_shipping': variant['requires_shipping'],
                                    'taxable': variant['taxable'],
                                    'available': variant['available'],
                                    'name': variant['name'],
                                    'public_title': variant['public_title'],
                                    'options': variant['options'],
                                    'price': variant['price'] + current_product['Battery Price'],
                                    'weight': variant['weight'],
                                    'compare_at_price': variant['compare_at_price'],
                                    'inventory_management': variant['inventory_management'],
                                    'barcode': variant['barcode'],
                                    'requires_selling_plan': variant['requires_selling_plan'],
                                    'selling_plan_allocations': variant['selling_plan_allocations']
                                }

                                if variant['featured_image'] is not None:
                                    additional_option['featured_image'] = {'id': variant['featured_image']['id'],
                                        'product_id': variant['featured_image']['product_id'],
                                        'position': variant['featured_image']['position'],
                                        'created_at': variant['featured_image']['created_at'],
                                        'updated_at': variant['featured_image']['updated_at'],
                                        'alt': variant['featured_image']['alt'],
                                        'width': variant['featured_image']['width'],
                                        'height': variant['featured_image']['height'],
                                        'src': variant['featured_image']['src'],
                                        'variant_ids': variant['featured_image']['variant_ids']
                                    }

                                    additional_option['featured_media'] = {'alt': variant['featured_media']['alt'],
                                        'id': variant['featured_media']['id'],
                                        'position': variant['featured_media']['position'],
                                        'preview_image': {'aspect_ratio': variant['featured_media']['preview_image']['aspect_ratio'],
                                            'height': variant['featured_media']['preview_image']['height'],
                                            'width': variant['featured_media']['preview_image']['width'],
                                            'src': variant['featured_media']['preview_image']['src']
                                        }
                                    }

                                else:
                                    additional_option['featured_image'] = None
                                    additional_option['featured_media'] = None

                                current_product['Variants'].append(additional_option)

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
                else:
                    print("Product data not found in the script.")
            else:
                print("Relevant script tag not found.")

            # sku_elem = product_elem.css_first('div.product-meta > div.product-meta__reference > span.product-meta__sku > span.product-meta__sku-number')
            # if sku_elem is not None:
            #     current_product['SKU'] = sku_elem.text(strip=True)

            # cost_elem = tree.css_first('div.product-form__info-content > div.price-list > span.price.price--highlight')
            # if cost_elem is not None:
            #     current_product['Cost per item'] = self.extract_price(cost_elem.text(strip=True))
            # else:
            #     cost_elem = tree.css_first('div.product-form__info-content > div.price-list > span.price')
            #     if cost_elem is not None:
            #         current_product['Cost'] = self.extract_price(cost_elem.text(strip=True))

            # current_product['Price'] = self.get_price(current_product['Cost'])

            # compare_at_price_elem = tree.css_first('div.product-form__info-content > div.price-list > span.price.price--compare')
            # if compare_at_price_elem is not None:
            #     current_product['CompareAtPrice'] = self.extract_price(compare_at_price_elem.text(strip=True))

            image_elems = tree.css('a.product-gallery__thumbnail')
            image_src_list = list()
            image_alt_text_list = list()
            for elem in image_elems:
                image_src_list.append(urljoin(self.base_url, elem.attributes.get('href').split('?')[0]))
                image_alt_text_list.append(elem.css_first('img').attributes.get('alt'))
            current_product['Image Src'] = image_src_list
            # current_product['Image Alt Text'] = image_alt_text_list
            current_product['Image Alt Text'] = ''

            video_elems = desc_elem.css('iframe')
            video_list = list()
            if video_elems is not None:
                for video_elem in video_elems:
                    video_src = video_elem.attributes.get('src')
                    video_list.append(video_src)
                current_product['Video Src'] = ','.join(video_list)

            # rating_elem = tree.css_first('div#stamped-main-widget')
            # if rating_elem is not None:
            #     print(rating_elem.html)
                # current_product['Product rating count (product.metafields.reviews.rating_count)'] = rating_elem.attributes.get('data-rating')

            product_datas.append(current_product)

            # Using Pandas
            df = pd.DataFrame.from_records(product_datas)

            # Breakdown Variants
            df = df.explode('Variants', ignore_index=True)
            df = self.extract_variants(df)


            variant_row_unused_columns = ['Title', 'Body (HTML)', 'Vendor', 'Product Category', 'Type', 'Tags', 'Published', 'Option1 Name',
                'Option2 Name', 'Option3 Name', 'Image Src', 'Image Alt Text', 'Gift Card', 'SEO Title', 'SEO Description',
                'Google Shopping / Google Product Category', 'Google Shopping / Gender', 'Google Shopping / Age Group',
                'Google Shopping / MPN', 'Google Shopping / Condition', 'Google Shopping / Custom Product',
                'Google Shopping / Custom Label 0', 'Google Shopping / Custom Label 1', 'Google Shopping / Custom Label 2',
                'Google Shopping / Custom Label 3', 'Google Shopping / Custom Label 4', 'Collection URL (product.metafields.custom.collection_url)',
                'enable_best_price (product.metafields.custom.enable_best_price)', 'Google: Custom Product (product.metafields.mm-google-shopping.custom_product)',
                'Product rating count (product.metafields.reviews.rating_count)', 'Included / United States', 'Price / United States',
                'Compare At Price / United States', 'Included / International', 'Price / International', 'Compare At Price / International', 'Status'
            ]

            df.loc[df.duplicated('Handle', keep='first'), variant_row_unused_columns] = ''

            df = df.explode('Image Src', ignore_index=True)

            images_row_unused_columns = ['Title', 'Body (HTML)', 'Vendor', 'Product Category', 'Type', 'Tags',
                'Published', 'Option1 Name', 'Option1 Value', 'Option2 Name', 'Option2 Value', 'Option3 Name', 'Option3 Value', 'Variant SKU',
                'Variant Grams', 'Variant Inventory Tracker', 'Variant Inventory Qty', 'Variant Inventory Policy', 'Variant Fulfillment Service',
                'Variant Price', 'Variant Compare At Price', 'Variant Requires Shipping', 'Variant Taxable', 'Variant Barcode',
                'Image Position', 'Image Alt Text', 'Gift Card', 'SEO Title', 'SEO Description', 'Google Shopping / Google Product Category',
                'Google Shopping / Gender', 'Google Shopping / Age Group', 'Google Shopping / MPN', 'Google Shopping / Condition',
                'Google Shopping / Custom Product', 'Google Shopping / Custom Label 0', 'Google Shopping / Custom Label 1', 'Google Shopping / Custom Label 2',
                'Google Shopping / Custom Label 3', 'Google Shopping / Custom Label 4', 'Collection URL (product.metafields.custom.collection_url)',
                'enable_best_price (product.metafields.custom.enable_best_price)', 'Google: Custom Product (product.metafields.mm-google-shopping.custom_product)',
                'Product rating count (product.metafields.reviews.rating_count)', 'Variant Image', 'Variant Weight Unit', 'Variant Tax Code', 'Cost per item',
                'Included / United States', 'Price / United States', 'Included / United States', 'Price / United States', 'Compare At Price / United States',
                'Included / International', 'Price / International', 'Compare At Price / International', 'Status', 'Video Src', 'Video Name', 'Variants'
            ]

            df.loc[df.duplicated('Variant SKU', keep='first'), images_row_unused_columns] = ''

            df.drop(columns=['Variants', 'Battery Option Value', 'Battery Price'])

            df.to_csv('products_output.csv', index=False, sep=',')

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
    urls = ['https://hobbytron.com/products/sonic-rc-helicopter', 'https://hobbytron.com/products/millennium-falcon-motion-sensing-quadcopter-by-world-tech-toys', 'https://hobbytron.com/products/luggage-playsets', 'https://hobbytron.com/products/ford-f-150-police-monster-struck-and-ford-f-250-super-duty-rc-truck-1-14-scale-bundle', 'https://hobbytron.com/products/buzz-lightyear-mickey-mouse-electronic-tabletop-basketball-playset']

    hs = HobbytronScraper()
    # products_htmls = asyncio.run(hs.fetch_all(urls))
    # hs.insert_to_db(products_htmls)
    hs.get_data()
