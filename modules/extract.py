"""This module contains the functions to extract the data."""
import re
import warnings
from time import sleep
import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim

warnings.filterwarnings("ignore")

def get_location(latitude, longitude):
    """Create a location object from latitude and longitude."""

    geolocator = Nominatim(user_agent="my_geocoder")

    location = geolocator.reverse(f"{latitude}, {longitude}")

    if location and location.raw:
        address = location.raw.get('address', {})
        calle = address.get('road', 'N/A')  # Calle
        barrio = address.get('neighbourhood', 'N/A')  # barrio
        sector = address.get('suburb', 'N/A')  # Sector
        ciudad = address.get('city', 'N/A')  # Ciudad
        dep = address.get('state', 'N/A')  # Ciudad

        direccion = f"{calle}, barrio {barrio}, {sector}, {ciudad} - {dep}"
    else:
        direccion = "N/A"

    return direccion, ciudad, barrio, sector, dep

def make_request(url, retries=3, timeout=50):
    """Realiza una solicitud HTTP con manejo de reintentos."""
    for attempt in range(retries):
        response = requests.get(url, timeout=timeout)
        if response.status_code == 200:
            return response
        print(f"Error {response.status_code}. Reintentando... ({attempt+1}/{retries})")
        sleep(2)  # Espera entre intentos
    print(f"No se pudo acceder a {url} después de {retries} intentos.")
    return None

def extract_href(inmueble, max_retries=2):
    """Extrae el href de un inmueble con reintentos."""
    for attempt in range(max_retries + 1):
        try:
            href_tag = inmueble.find('a', href=True)
            return href_tag['href'] if href_tag else None
        except Exception as e:
            print(f"Error al extraer href: {e}. Reintentando... ({attempt+1}/{max_retries})")
    return None

def extract_text(soup, tag, class_, replace_text=None):
    """Extract and clean text from a tag."""
    try:
        element = soup.find(tag, class_=class_)
        if element:
            text = element.text.strip()
            return text.replace(replace_text, '') if replace_text else text
        return 'N/A'
    except Exception as e:
        print(f"Error al extraer {class_}: {e}")
        return 'N/A'

def scrape_main_page(url, page, max_pages, pages_url):
    """Extract main data from properties."""
    properties = []
    types = ['bussines_type=Venta', 'bussines_type=Arrendar']

    while page <= max_pages:
        for type_url in types:
            print(f"Extrayendo inmuebles de la página {page} para {type_url}...")
            full_url = f"{url}{pages_url}{page}&{type_url}"
            response = make_request(full_url)

            if not response:
                page += 1
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            properties_div = soup.find('div', id='properties')

            if not properties_div:
                print(f"No se encontraron propiedades en la página {page}. Terminando...")
                break

            for inmueble in properties_div.find_all('div', class_='property-card'):
                href = extract_href(inmueble)
                if not href:
                    continue

                tipo_inmu = extract_text(inmueble, 'p', 'tipo-inmueble', replace_text='Tipo: ')
                tipo_nego = extract_text(inmueble, 'p', 'tipo-negocio', replace_text='Tipo: ')

                property_info = {
                    'link': href,
                    'tipo_inmueble': tipo_inmu,
                    'tipo_negocio': tipo_nego
                }

                properties.append(property_info)

        page += 1

    return properties


def extract_info(soup, tag, id=None, class_=None, text=None, is_span=True, span_class='attr-value', parent_tag=None):
    """Extrae detalles de una propiedad según los parámetros proporcionados."""
    retries = 0
    max_retries = 2

    while retries < max_retries:
        try:
            if id:
                tag_found = soup.find(tag, id=id)
            elif class_:
                tag_found = soup.find(tag, class_=class_)
                if text:
                    tag_found = soup.find(tag, class_=class_, string=text)
                else:
                    tag_found = soup.find(tag, class_=class_)
            else:
                return 0

            if not tag_found:
                return 0

            if parent_tag:
                parent_div = tag_found.find_parent(parent_tag, class_='col-6')
                if parent_div:
                    span = parent_div.find('span', class_=span_class)
                    return span.text.strip() if span else 'N/A'

            if is_span:
                span = tag_found.find('span', class_=span_class)
                return span.text.strip() if span else 0

            return tag_found.text.strip()

        except Exception as e:
            print(f"Error al extraer información de {tag} con {id or class_}: {e}")
            retries += 1

def extract_contact(soup, tag, class_=None):
    """Extract contact information."""
    try:
        contact_tag = soup.find(tag, class_=class_)
        
        if contact_tag:
            contact_link = contact_tag.find('a', href=True)
            contact = contact_link['href'] if contact_link else 'N/A'
        else:
            contact = 'N/A'

        return contact
    except AttributeError as e:
        print(f"Error al buscar el tag 'div' para el contacto: {e}")
        contact = 'N/A'
    except TypeError as e:
        print(f"Error: 'soup' es None o no tiene un tag 'div' para el contacto: {e}")
        contact = 'N/A'
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        contact = 'N/A'

def extract_location(script_text):
    """Extrae la latitud y longitud para determinar la ubicación."""
    try:
        # Corrección en la expresión regular para 'longitude'
        lat_match = re.search(r'latitude\s*=\s*([-+]?\d*\.\d+|\d+);', script_text)
        lon_match = re.search(r'longitude\s*=\s*([-+]?\d*\.\d+|\d+);', script_text)

        latitude = lat_match.group(1) if lat_match else 'N/A'
        longitude = lon_match.group(1) if lon_match else 'N/A'
        return latitude, longitude

    except re.error as e:
        print(f"Error en la expresión regular: {e}")
        return 'N/A', 'N/A'

def extract_images(soup):
    """Extract images URLs."""
    image_urls = []
    try:
        slider_tag = soup.find('div', id='property-slider')
        if slider_tag:
            list_items = slider_tag.find_all('li')
            for item in list_items:
                img_div = item.find('div', class_='prop-preview-img')
                if img_div:
                    style_attr = img_div.get('style', '')
                    match = re.search(r'url\((.*?)\)', style_attr)
                    if match:
                        image_urls.append(match.group(1))
    except Exception as e:
        print(f"Error al extraer imágenes: {e}")
    return image_urls

def scrape_property_page(url, properties):
    """Scrape a property page and return the data of each property."""
    for property_info in properties:
        link = property_info.get('link')
        response = requests.get(url + str(link), timeout=50)
        soup = BeautifulSoup(response.text, 'html.parser')

        print(f"Extrayendo información de la propiedad: {link}")

        habitaciones = extract_info(soup, 'div', id='alcoba', is_span=True, span_class='attr-value')
        banios = extract_info(soup, 'div', id='banios', is_span=True, span_class='attr-value')
        precio = extract_info(soup, 'li', class_='precio', is_span=True, span_class='second')
        area = extract_info(soup, 'li', class_='area', is_span=True, span_class='second')
        
        estrato = extract_info(soup, 'li', class_='estrato', is_span=True, span_class='second')
        closet = extract_info(soup, 'div', id='closet', is_span=True, span_class='attr-value')
        garaje = extract_info(soup, 'div', class_='attr-name titulo', text='Garaje', span_class='attr-value text', parent_tag='div')

        script_tag = soup.find('script', text=re.compile(r'latitude'))
        if script_tag:
            script_text = script_tag.text
            latitude, longitude = extract_location(script_text)
        else:
            latitude, longitude = 0, 0

        image_urls = extract_images(soup)

        try:
            property_info.update({
                'habitaciones': habitaciones,
                'banios': banios,
                'precio': precio,
                'area': area,
                'contacto': extract_contact(soup, 'div', class_='wap mt-2'),
                'direccion': get_location(latitude, longitude)[0],
                'ciudad': get_location(latitude, longitude)[1],
                'barrio': get_location(latitude, longitude)[2],
                'sector': get_location(latitude, longitude)[3],
                'departamento': get_location(latitude, longitude)[4],
                'otras_caracteristicas': {"Estrato": estrato, "Closet": closet, "Garaje": garaje},
                'imagenes': image_urls
            })
        except Exception as e:
            print(f"Ocurrió un error al agregar información a property_info: {e}")

        sleep(1)

    return properties
