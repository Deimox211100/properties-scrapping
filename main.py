"""Main module to run the scrapper"""
from time import time
from modules.extract import scrape_main_page, scrape_property_page
from modules.transform import transform_data
from modules.load import load_arriendos, load_ventas

URL = 'https://www.arrendamientossantafe.com'
PAGES_URL = '/propiedades/?page='
PAGE = 1
MAX_PAGE = 10
TABLE_ARRIENDOS = 'deimox-dw.pruebas.arriendos'
TABLE_VENTAS = 'deimox-dw.pruebas.ventas'

if __name__ == '__main__':
    start = time()
    print('Iniciando scraping de', URL ,'Maximo:', MAX_PAGE, 'páginas')
    properties = scrape_main_page(URL, PAGE, MAX_PAGE, PAGES_URL)
    properties = scrape_property_page(URL, properties)
    arriendo, venta = transform_data(properties)
    load_arriendos(arriendo, TABLE_ARRIENDOS)
    load_ventas(venta, TABLE_VENTAS)
    finish = time()
    elapsed_time = finish - start
    print(f'Scraping finalizado en {elapsed_time:.2f} segundos.')