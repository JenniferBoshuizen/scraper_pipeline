import logging
import os
import csv
import pyodbc
import pandas as pd
from gazpacho import get, Soup
from datetime import date
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO

app = func.FunctionApp()

load_dotenv()
# Blob storage Configuration
BLOB_STORAGE_NAME = "scrapeddatabit"
BLOB_STORAGE_CONNECTIONSTRING = os.getenv('AzureWebJobsscrapeddatabit_STORAGE')
BLOB_STORAGE_CONTAINER_NAME = "scrapedblobs"

# ---- Azure function nummer 1 -----

# Bol
def scrape_bol_prices():
    today = date.today()
    base_url = "https://www.bol.com/nl/nl/l/smartphones/4010/?page="
    page_number = 1
    scraped_data = []

    while True:
        url = f"{base_url}{page_number}"
        logging.info(f"Current page: {str(page_number)}")
        response = get(url)
        soup = Soup(response)

        product_list = soup.find('div', {'data-test': 'product-content'}, mode='all')

        if not product_list:
            break

        for item in product_list:
            try:
                try:
                    price = item.find('span', {'class': 'promo-price'}, mode='first').text.strip()
                    price_fraction = item.find('sup', {'class': 'promo-price__fraction'}, mode='first').text.strip()
                    price = float(price) if price_fraction == '-' else float(price + '.' + price_fraction)
                except AttributeError:
                    logging.warning("Price or price fraction not found for a product on Bol")
                    break
                
                try:
                    title = item.find('a', {'data-test': 'product-title'}, mode='first').text.strip()
                    brand = item.find('a', {'data-test': 'party-link'}, mode='first').text.strip()
                except:
                    logging.warning("Title or Brand not found for a product on Bol")
                    break

                if title.startswith(brand):
                    title = title[len(brand):].strip()

                try:
                    seller_elem1 = item.find('span', {'class': 'product-seller__name'})
                    seller_elem2 = item.find('div', {'data-test': 'plazaseller-link'})
                    if seller_elem1 or seller_elem2:
                        seller = "Bol"
                    else: 
                        seller = "Tweedehands op Bol"
                except AttributeError:
                    logging.warning("Seller not found for a product on Bol")

                if price is not None:
                    scraped_data.append([brand, title, price, seller, today])

            except AttributeError as ae:
                logging.error(f"AttributeError parsing product on Bol: {ae}")
            except ValueError as ve:
                logging.error(f"ValueError parsing product on Bol: {ve}")
            except Exception as e:
                logging.error(f"Unexpected error parsing product on Bol: {e}")

        next_button = soup.find('li', {'class': '[ pagination__controls pagination__controls--next ] js_pagination_item'}, mode='first')
        if not next_button:
            break

        page_number += 1

    logging.info("Klaar met Bol :)")
    return scraped_data

# Coolblue
def scrape_coolblue_prices():
    today = date.today()
    base_url = "https://www.coolblue.nl/mobiele-telefoons/smartphones?pagina="
    page_number = 1
    scraped_data = []
    
    while True:
        url = f"{base_url}{page_number}"
        logging.info(f"Current page: {str(page_number)}")
        response = get(url)
        soup = Soup(response)

        product_list = soup.find('div', {'class': 'product-card__details product-card__custom-breakpoint js-product-details'}, mode='all')

        if not product_list:
            break

        for item in product_list:
            try:
                title = item.find('a', {'class': 'link'}, mode='first').text.strip().split('+')[0].strip()
                brand = title.split(' ')[0]

                if title.startswith(brand):
                    title = title[len(brand):].strip()
                if title == 'reviews': break

                price = None
                try:
                    price_text = item.find('strong', {'class': 'sales-price__current js-sales-price-current'}, mode='first').text.strip().replace('.', '')
                    price_pieces = price_text.split(',')
                    price = float(price_pieces[0]) if price_pieces[1] == '-' else float(price_text.replace(',', '.'))
                except AttributeError:
                    logging.warning("Price not found for a product on Coolblue")

                seller = "Coolblue"

                if price is not None:
                    scraped_data.append([brand, title, price, seller, today])

            except AttributeError as ae:
                logging.error(f"AttributeError parsing product on Coolblue: {ae}")
            except ValueError as ve:
                logging.error(f"ValueError parsing product on Coolblue: {ve}")
            except Exception as e:
                logging.error(f"Unexpected error parsing product on Coolblue: {e}")

        next_button = soup.find('a', {'aria-label': 'Ga naar de volgende pagina'}, mode='first')
        if not next_button:
            break

        page_number += 1

    logging.info("Klaar met Coolblue :)")
    return scraped_data

# Mobiel.nl
def scrape_mobiel_prices():
    today = date.today()
    base_url = "https://www.mobiel.nl/smartphone?page="
    page_number = 1
    scraped_data = []

    while True:
        url = f"{base_url}{page_number}"
        logging.info(f"Current page: {str(page_number)}")
        response = get(url)
        soup = Soup(response)

        product_list = soup.find('div', {'class': 'Card-sc-ee1jox-0 InteractiveCard-sc-vn9il8-0 ProductOnlyCard__StyledInteractiveCard-sc-1dljfv1-5 kuTVLb dUaVxg GnNok'}, mode='all')

        if not product_list:
            break

        for item in product_list:
            try:
                brand_elem = item.find('span', {'class': 'ProductTitle__Brand-sc-1bzuqdo-2 bZdkAg'}, mode='first')
                brand = brand_elem.text.strip() if brand_elem else "Unknown Brand"

                title_elem = item.find('span', {'class': 'ProductTitle__Title-sc-1bzuqdo-3 dUSavn'}, mode='first')
                title = title_elem.text.strip() if title_elem else "Unknown Title"

                price = None
                try:
                    price_elem = item.find('div', {'class': 'Bottom__Prices-sc-s5inlj-3 hsHRsW'}, mode='first')
                    if price_elem:
                        price_span = price_elem.find('span', mode='first')
                        price = int(price_span.text.strip().replace('.', '')) if price_span else None
                except AttributeError:
                    logging.warning("Price not found for a product on Mobiel.nl")

                seller = "Mobiel.nl"

                if price is not None:
                    scraped_data.append([brand, title, price, seller, today])

            except AttributeError as ae:
                logging.error(f"AttributeError parsing product on Mobiel.nl: {ae}")
            except ValueError as ve:
                logging.error(f"ValueError parsing product on Mobiel.nl: {ve}")
            except Exception as e:
                logging.error(f"Unexpected error parsing product on Mobiel.nl: {e}")

        next_button = soup.find('a', {'rel': 'next'}, mode='first')
        if not next_button:
            break

        page_number += 1

    logging.info("Klaar met Mobiel.nl :)")
    return scraped_data

# CSV maken en uploaden naar Azure Blob Storage
def scraped_to_blob(site_name, scraped_data, BLOB_STORAGE_CONTAINER_NAME, BLOB_STORAGE_CONNECTIONSTRING):
    today = date.today()
    csv_filename = f"{today}_{site_name}_products.csv"
    
    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(['Brand', 'Title', 'Price', 'Seller', 'Date'])
    csv_writer.writerows(scraped_data)
    csv_data = output.getvalue()
    output.close()

    blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTIONSTRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_STORAGE_CONTAINER_NAME, blob=csv_filename)
    blob_client.upload_blob(csv_data, overwrite=True)

@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def Daily_scrape(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    scraped_bol_data = scrape_bol_prices()
    scraped_to_blob('bol', scraped_bol_data, BLOB_STORAGE_CONTAINER_NAME, BLOB_STORAGE_CONNECTIONSTRING)

    scraped_coolblue_data = scrape_coolblue_prices()
    scraped_to_blob('coolblue', scraped_coolblue_data, BLOB_STORAGE_CONTAINER_NAME, BLOB_STORAGE_CONNECTIONSTRING)

    scraped_mobiel_data = scrape_mobiel_prices()    
    scraped_to_blob('mobiel', scraped_mobiel_data, BLOB_STORAGE_CONTAINER_NAME, BLOB_STORAGE_CONNECTIONSTRING)

    logging.info(f'Scraped data saved to blob: {BLOB_STORAGE_NAME}')

# ---- Azure function nummer 2 -----
@app.function_name(name="Blob_to_sql")
@app.blob_trigger(arg_name="myblob", path="scrapedblobs/{name}",
                               connection="AzureWebJobsscrapeddatabit_STORAGE") 
def Blob_to_sql(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    # Lezen van de blob inhoud
    blob_data = myblob.read().decode('utf-8')
    
    # Dataframe maken van de CSV inhoud
    data = StringIO(blob_data)
    df = pd.read_csv(data)
    
    # Verbinding details
    SQL_SERVER = os.getenv("sql_server_name")
    SQL_DATABASE = os.getenv("sql_db_name")
    SQL_USER = os.getenv("sql_server_admin")
    SQL_PASSWORD = os.getenv("sql_server_password")
    
    # Verbinding maken met SQL Server
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD}"
    )
    cursor = conn.cursor()

    # Gegevens invoegen in de SQL Server tabel
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO Products (Brand, Title, Price, Seller, Date)
            VALUES (?, ?, ?, ?, ?)
        """, row['Brand'], row['Title'], row['Price'], row['Seller'], row['Date'])

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Data inserted into SQL Database successfully.")

