import re
import requests
import pandas as pd
import time
import unicodedata
import random
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

# configuracoes
GOOGLE_API_KEY = "API_KEY"
SEARCH_ENGINE_ID = "ENGINE_KEY
ARQUIVO_ENTRADA = "Planilha sem título (1).xlsx"
ARQUIVO_SAIDA = "empresas_enriquecidas.xlsx"

# Lista de User Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
]


# Funcao para normalizacao de textos
def normalizar_nome(texto):
    texto_str = str(texto) if texto is not None else ""
    texto_str = texto_str.strip().lower()
    nfkd_form = unicodedata.normalize('NFKD', texto_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def delay_aleatorio(min_seg=2, max_seg=5):
    time.sleep(random.uniform(min_seg, max_seg))

# Simular movimento de mouse
def mover_mouse_aleatorio(driver):
    try:
        action = ActionChains(driver)
        action.move_by_offset(
            random.randint(0, 100),
            random.randint(0, 100)
        ).perform()
        action.reset_actions()
    except:
        pass


# configuracoes do driver
def configurar_driver():
    chrome_options = Options()

    chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--start-maximized")

    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=chrome_options)

    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": driver.execute_script("return navigator.userAgent").replace('Headless', '')
    })
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
    driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['pt-BR', 'pt', 'en-US', 'en']})")

    return driver

# Busca CNPJ no Portal da Transparência
def buscar_cnpj_transparencia(driver, nome_empresa):
    try:
        print(f"Tentando Portal da Transparência para: {nome_empresa}")

        wait = WebDriverWait(driver, 15)
        termo_encoded = quote(nome_empresa)
        url = f"https://portaldatransparencia.gov.br/busca?termo={termo_encoded}&pessoaJuridica=true"

        driver.get(url)
        delay_aleatorio(3, 5)

        mover_mouse_aleatorio(driver)

        # Busca o link do primeiro resultado
        link_resultado = wait.until(
            EC.presence_of_element_located((
                By.XPATH,
                "/html/body/main/div/div[2]/section/div/div/div[1]/div[2]/ul/div[1]/h4/a"
            ))
        )

        href = link_resultado.get_attribute("href")

        # extrai o CNPJ
        match = re.search(r'/pessoa-juridica/(\d+)-', href)
        if match:
            cnpj = match.group(1)
            print(f"CNPJ encontrado no Portal da Transparência: {cnpj}")
            return cnpj

        return None

    except Exception as e:
        print(f"Erro no Portal da Transparência: {e}")
        return None


# Busca o CNPJ da empresa no ConsultasCNPJ
def buscar_cnpj_consultascnpj(driver, nome_empresa):
    try:
        wait = WebDriverWait(driver, 15)

        driver.get("https://www.consultascnpj.com/")
        delay_aleatorio(2, 4)

        mover_mouse_aleatorio(driver)

        input_box = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='text']"))
        )

        # Digitar com delay entre caracteres
        input_box.clear()
        for char in nome_empresa:
            input_box.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))

        delay_aleatorio(1, 2)

        mover_mouse_aleatorio(driver)

        botao_buscar = driver.find_element(
            By.XPATH,
            "/html/body/main/div/div[1]/form/table/tbody/tr/td[2]/button"
        )
        botao_buscar.click()

        delay_aleatorio(2, 4)

        # primeiro resultado
        resultado = wait.until(
            EC.presence_of_element_located((
                By.XPATH,
                "/html/body/main/div/div[2]/div/div/div[1]/div[6]/div[2]/div/div/div[1]/div[1]/div/div[1]/div/a"
            ))
        )

        url_resultado = resultado.get_attribute("href")

        # se for de matriz, acessa o link e busca a 1° filial
        if "/matriz/" in url_resultado:
            driver.get(url_resultado)
            delay_aleatorio(2, 3)

            link_matriz = wait.until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/main/div/div/section/div/a[1]"
                ))
            )

            url_matriz = link_matriz.get_attribute("href")
            cnpj = re.search(r'(\d{14})', url_matriz)
            return cnpj.group(1) if cnpj else None

        # caso normal
        cnpj = re.search(r'(\d{14})', url_resultado)
        return cnpj.group(1) if cnpj else None

    except Exception as e:
        print(f"Erro no ConsultasCNPJ: {e}")
        return None


# Função principal de busca de CNPJ
def buscar_cnpj(driver, nome_empresa):
    # Tenta primeiro no ConsultasCNPJ
    print(f"Tentando ConsultasCNPJ para: {nome_empresa}")
    cnpj = buscar_cnpj_consultascnpj(driver, nome_empresa)

    if cnpj:
        return cnpj

    # Se falhar, tenta no Portal da Transparência
    print("ConsultasCNPJ falhou, tentando Portal da Transparência...")
    delay_aleatorio(2, 3)
    cnpj = buscar_cnpj_transparencia(driver, nome_empresa)

    return cnpj


# Busca o site da empresa via google api
def buscar_site_google(razao_social, cidade):
    url = "https://www.googleapis.com/customsearch/v1"
    termo = re.sub(r'\b(LTDA|S\.A\.?|S/A|LIMITADA|EIRELI|ME|EPP)\b', '', razao_social, flags=re.IGNORECASE).strip()
    query = f'"{termo}" {cidade} site oficial'

    params = {'q': query, 'key': GOOGLE_API_KEY, 'cx': SEARCH_ENGINE_ID, 'num': 3, 'gl': 'br'}
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            items = res.json().get('items', [])
            blacklist = ["econodata", "casadosdados", "cnpj.biz", "jusbrasil", "transparencia.cc"]
            for item in items:
                if not any(b in item['link'].lower() for b in blacklist):
                    return item['link']
    except Exception as e:
        print(f"[ERRO Google API] {e}")
    return "Não encontrado"


# Funcao para normalizar o qsa no resultado da brasilapi
def normalizar_qsa(qsa, max_socios=3):
    resultado = {}

    if not isinstance(qsa, list):
        return resultado

    for i, socio in enumerate(qsa[:max_socios], start=1):
        resultado[f"socio_{i}_nome"] = socio.get("nome_socio")
        resultado[f"socio_{i}_qualificacao"] = socio.get("qualificacao_socio")
        resultado[f"socio_{i}_cpf_rep_legal"] = socio.get("cpf_representante_legal")
        resultado[f"socio_{i}_nome_rep_legal"] = socio.get("nome_representante_legal")
        resultado[f"socio_{i}_qualificacao_rep_legal"] = socio.get(
            "qualificacao_representante_legal"
        )

    resultado["quantidade_socios"] = len(qsa)
    return resultado


# carregando base de empresas
def processar_base():
    try:
        df_input = pd.read_excel(ARQUIVO_ENTRADA)
    except Exception as e:
        print(f"Erro ao ler Excel: {e}")
        return

    driver = configurar_driver()
    resultados_finais = []

    for index, row in df_input.iterrows():
        nome_original = row['company_name']
        nome_busca_normalizado = normalizar_nome(nome_original)

        print(f"[{index + 1}/{len(df_input)}] Processando empresa: {nome_original}")

        info_empresa = row.to_dict()
        info_empresa["Site Encontrado"] = "Não encontrado"

        # busca cnpj com fallback automático
        cnpj = buscar_cnpj(driver, nome_busca_normalizado)

        if cnpj:
            cnpj = str(cnpj).zfill(14)
            print(f"✓ CNPJ encontrado: {cnpj}")

            # Delay antes da API
            delay_aleatorio(1, 2)

            # Busca dos dados cadastrais na brasilapi
            try:
                res_api = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=15)
                if res_api.status_code == 200:
                    dados_cnpj = res_api.json()

                    qsa = dados_cnpj.pop("qsa", [])

                    # normaliza dados principais
                    df_temp = pd.json_normalize(dados_cnpj)
                    dados_normalizados = df_temp.to_dict(orient="records")[0]
                    info_empresa.update(dados_normalizados)

                    # normaliza qsa
                    info_empresa.update(normalizar_qsa(qsa))

                    # googleapi
                    razao = info_empresa.get('razao_social', nome_busca_normalizado)
                    cidade = info_empresa.get('municipio', '')
                    print(f"Buscando site para: {razao}")
                    info_empresa["Site Encontrado"] = buscar_site_google(razao, cidade)
                    print(f"Site: {info_empresa['Site Encontrado']}")
                else:
                    print(f"BrasilAPI status {res_api.status_code}")
                    info_empresa["Erro_Log"] = f"BrasilAPI Status {res_api.status_code}"
            except Exception as e:
                print(f"Erro na BrasilAPI: {e}")
                info_empresa["Erro_Log"] = str(e)
        else:
            print("CNPJ não encontrado em nenhuma fonte")
            info_empresa["Erro_Log"] = "CNPJ não encontrado"

        resultados_finais.append(info_empresa)

        # Delay entre empresas para evitar bloqueios
        delay_time = random.uniform(4, 8)
        print(f"\nAguardando {delay_time:.1f}s antes da próxima empresa...")
        time.sleep(delay_time)

    driver.quit()

    # gerando df final
    df_final = pd.DataFrame(resultados_finais)

    cols = list(df_final.columns)
    if "Site Encontrado" in cols:
        cols.insert(1, cols.pop(cols.index("Site Encontrado")))
        df_final = df_final[cols]

    if 'cnpj' in df_final.columns:
        df_final['cnpj'] = df_final['cnpj'].apply(lambda x: f"'{x}" if pd.notnull(x) and x != "" else x)

    df_final.to_excel(ARQUIVO_SAIDA, index=False)
    print(f"\n{'=' * 60}")
    print(f"processo concluido")
    print(f"arquivo final: {ARQUIVO_SAIDA}")


if __name__ == "__main__":
    processar_base()
