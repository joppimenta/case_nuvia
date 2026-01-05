import requests
from bs4 import BeautifulSoup
import time
import random
import csv
from datetime import datetime
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass, asdict
from urllib.parse import urlencode
import hashlib
import os
import glob

# registro de execução (log)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# estrutura de dados da vaga
@dataclass
class JobListing:
    job_id: str
    title: str
    company: str
    location: str
    description: str
    posted_date: str
    url: str
    scraped_at: str
    search_keyword: str

# Rate Limiter, controla a velocidade das requisicoes HTTP
class RateLimiter:
    # Tempo entre requisicoes
    def __init__(self, min_delay=2, max_delay=10):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.current_delay = min_delay
        self.last_request = 0

    # Aguarda tempo necessário antes da próxima requisição"
    def wait(self):
        elapsed = time.time() - self.last_request
        if elapsed < self.current_delay:
            sleep_time = self.current_delay - elapsed
            jitter = random.uniform(0, 0.5)  # Adiciona jitter
            time.sleep(sleep_time + jitter)
        self.last_request = time.time()

    # Se bloquear, dobra o tempo de delay
    def increase_delay(self):
        self.current_delay = min(self.current_delay * 2, self.max_delay)
        logging.warning(f"Rate limit hit. Aumentando delay para {self.current_delay}s")

    # Se der certo, reseta o  tempo de delay
    def reset_delay(self):
        """Reseta delay após sucesso"""
        self.current_delay = self.min_delay


class RequestHandler:

    # Simulação de requisicoes vindas por diferentes tipos de usuários/maquinas
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
    ]

    def __init__(self, max_retries=3):
        self.max_retries = max_retries
        self.session = requests.Session()

    # Header de requisicao
    def get_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }

    # Requisicao com os rate limiters
    def make_request(self, url: str, rate_limiter: RateLimiter) -> Optional[requests.Response]:
        for attempt in range(self.max_retries):
            try:
                rate_limiter.wait()

                response = self.session.get(
                    url,
                    headers=self.get_headers(),
                    timeout=15,
                    allow_redirects=True
                )

                if response.status_code == 200:
                    rate_limiter.reset_delay()
                    return response

                elif response.status_code == 429:
                    rate_limiter.increase_delay()
                    wait_time = (2 ** attempt) * 5
                    logging.warning(f"Rate limit (429). Aguardando {wait_time}s...")
                    time.sleep(wait_time)

                elif response.status_code in [403, 401]:
                    logging.error(f"Acesso negado ({response.status_code}). Aguardando...")
                    time.sleep(30)

                else:
                    logging.warning(f"Status {response.status_code} na tentativa {attempt + 1}")
                    time.sleep(2 ** attempt)

            except requests.exceptions.Timeout:
                logging.warning(f"Timeout na tentativa {attempt + 1}")
                time.sleep(2 ** attempt)

            except requests.exceptions.ConnectionError as e:
                logging.error(f"Erro de conexão: {e}")
                time.sleep(5 * (attempt + 1))

            except Exception as e:
                logging.error(f"Erro inesperado: {e}")
                time.sleep(5)

        logging.error(f"Falha após {self.max_retries} tentativas: {url}")
        return None

# Scraping do linkedin
class LinkedInJobsScraper:
    BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    def __init__(self):
        self.request_handler = RequestHandler()
        self.rate_limiter = RateLimiter(min_delay=3, max_delay=15)
        self.jobs_collected = []
        self.jobs_seen = set()
        self.new_jobs_count = 0

    def generate_job_id(self, title: str, company: str) -> str:
        unique_str = f"{title}|{company}|{datetime.now().date()}"
        return hashlib.md5(unique_str.encode()).hexdigest()[:16]

    def load_existing_csv(self, pattern: str = 'linkedin_jobs_*.csv') -> int:
        csv_files = glob.glob(pattern)

        if not csv_files:
            logging.info("CSV nao encontrado, gerando um novo")
            return 0

        # Carregando os dados do CSV
        latest_csv = max(csv_files, key=os.path.getctime)
        logging.info(f"Carregando dados existentes de: {latest_csv}")

        loaded_count = 0
        try:
            with open(latest_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    job = JobListing(
                        job_id=row['job_id'],
                        title=row['title'],
                        company=row['company'],
                        location=row['location'],
                        description=row['description'],
                        posted_date=row['posted_date'],
                        url=row['url'],
                        scraped_at=row['scraped_at'],
                        search_keyword=row['search_keyword']
                    )
                    self.jobs_collected.append(job)
                    self.jobs_seen.add(job.job_id)
                    loaded_count += 1

            logging.info(f"{loaded_count} vagas carregadas do histórico")
            return loaded_count

        except Exception as e:
            logging.error(f"Erro ao carregar CSV: {e}")
            return 0

    def build_search_url(self, keyword: str, location: str = "", start: int = 0) -> str:
        params = {
            'keywords': keyword,
            'location': location,
            'start': start,
            'sortBy': 'DD'
        }
        return f"{self.BASE_URL}?{urlencode(params)}"

    # Extrai os dados da vaga
    def parse_job_card(self, card_html, keyword: str) -> Optional[JobListing]:
        try:
            job_id = card_html.get('data-entity-urn', '').split(':')[-1]
            if not job_id:
                job_id = self.generate_job_id(
                    card_html.find('h3').text.strip() if card_html.find('h3') else '',
                    card_html.find('h4').text.strip() if card_html.find('h4') else ''
                )

            title_elem = card_html.find('h3', class_='base-search-card__title')
            company_elem = card_html.find('h4', class_='base-search-card__subtitle')
            location_elem = card_html.find('span', class_='job-search-card__location')
            date_elem = card_html.find('time')
            link_elem = card_html.find('a', class_='base-card__full-link')

            title = title_elem.text.strip() if title_elem else 'N/A'
            company = company_elem.text.strip() if company_elem else 'N/A'
            location = location_elem.text.strip() if location_elem else 'N/A'
            posted_date = date_elem.get('datetime', 'N/A') if date_elem else 'N/A'
            url = link_elem.get('href', '') if link_elem else ''

            desc_elem = card_html.find('p', class_='base-search-card__snippet')
            description = desc_elem.text.strip() if desc_elem else ''

            # limpeza de campos
            title = title.replace('\n', ' ').replace('\r', ' ')
            company = company.replace('\n', ' ').replace('\r', ' ')
            description = description.replace('\n', ' ').replace('\r', ' ')

            return JobListing(
                job_id=job_id,
                title=title,
                company=company,
                location=location,
                description=description,
                posted_date=posted_date,
                url=url,
                scraped_at=datetime.now().isoformat(),
                search_keyword=keyword
            )

        except Exception as e:
            logging.error(f"Erro ao parsear card: {e}")
            return None

    # Busca por vaga especifica em localidade definida
    def scrape_search(self, keyword: str, location: str = "", max_pages: int = 5) -> int:
        jobs_this_search = 0

        logging.info(f"iniciando scrape: keyword='{keyword}', location='{location}'")

        for page in range(max_pages):
            start = page * 25
            url = self.build_search_url(keyword, location, start)

            logging.info(f"página {page + 1}/{max_pages} (start={start})")

            response = self.request_handler.make_request(url, self.rate_limiter)

            if not response:
                logging.error(f"falha ao buscar página {page + 1}")
                break

            soup = BeautifulSoup(response.content, 'html.parser')
            job_cards = soup.find_all('li')

            if not job_cards:
                logging.info(f"nenhuma vaga encontrada na página {page + 1}")
                break

            page_jobs = 0
            for card in job_cards:
                job = self.parse_job_card(card, keyword)
                if job and job.job_id not in self.jobs_seen:
                    self.jobs_collected.append(job)
                    self.jobs_seen.add(job.job_id)
                    jobs_this_search += 1
                    page_jobs += 1
                    self.new_jobs_count += 1

            logging.info(f"Página {page + 1}: {page_jobs} vagas novas processadas")

            if page_jobs == 0:
                break

        logging.info(f"Scrape concluído")

        return jobs_this_search

    # Salva no csv
    def save_to_csv(self, filename: str):
        if not self.jobs_collected:
            logging.warning("Nenhuma vaga para salvar")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Cabeçalho
            writer.writerow([
                'job_id', 'title', 'company', 'location',
                'description', 'posted_date', 'url',
                'scraped_at', 'search_keyword'
            ])

            # Dados
            for job in self.jobs_collected:
                writer.writerow([
                    job.job_id,
                    job.title,
                    job.company,
                    job.location,
                    job.description,
                    job.posted_date,
                    job.url,
                    job.scraped_at,
                    job.search_keyword
                ])

        logging.info(f"Dados salvos em {filename}")

    def get_stats(self) -> Dict:
        companies = set(job.company for job in self.jobs_collected)
        keywords = set(job.search_keyword for job in self.jobs_collected)

        return {
            'total_jobs': len(self.jobs_collected),
            'new_jobs': self.new_jobs_count,
            'unique_companies': len(companies),
            'keywords_searched': len(keywords)
        }


def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f'linkedin_jobs_{timestamp}.csv'

    scraper = LinkedInJobsScraper()

    print("=" * 60)
    print("Iniciando scraper")
    print("=" * 60)

    # carrega historico
    existing_count = scraper.load_existing_csv()
    if existing_count > 0:
        print(f"{existing_count} vagas carregadas do histórico\n")

    # vagas a serem buscadas
    searches = [
        {'keyword': 'cfo', 'location': 'Brazil'},
        {'keyword': 'diretor financeiro', 'location': 'Brazil'},
        {'keyword': 'chefe de finanças', 'location': 'Brazil'}
    ]

    # Executa scraping para todas as buscas
    for i, search in enumerate(searches, 1):
        print(f"[{i}/{len(searches)}] buscando: {search['keyword']} em {search['location']}")
        jobs = scraper.scrape_search(
            keyword=search['keyword'],
            location=search['location'],
            max_pages=3
        )
        print(f"✅ {jobs} vagas novas coletadas\n")

        # Sleep entre buscas
        if i < len(searches):
            time.sleep(5)

    # salva csv
    scraper.save_to_csv(csv_filename)

    # estatisticas principais
    stats = scraper.get_stats()
    print("=" * 60)
    print("finalizando scraping")
    print(f"Total de vagas no banco: {stats['total_jobs']}")
    print(f"Vagas novas: {stats['new_jobs']}")
    print(f"\nDados salvos em: {csv_filename}")
    print("=" * 60)


if __name__ == '__main__':
    main()