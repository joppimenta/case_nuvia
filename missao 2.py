import re
import requests
import pandas as pd
import time
import unicodedata
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# configuracoes
GOOGLE_API_KEY = "AIzaSyBD4YNz03jxrNKmCcmFCw4Mm5mrQ2bZU8E"
SEARCH_ENGINE_ID = "e154946e6998c495c"
ARQUIVO_ENTRADA = "Planilha sem título (1).xlsx"
ARQUIVO_SAIDA = "empresas_enriquecidas.xlsx"

# Funcao para normalizacao de textos
def normalizar_nome(texto):
    texto_str = str(texto) if texto is not None else ""

    texto_str = texto_str.strip().lower()
    nfkd_form = unicodedata.normalize('NFKD', texto_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

# configuracoes do driver
def configurar_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # User agent mais realista
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )

    driver = webdriver.Chrome(options=chrome_options)

    # Remove indicadores de webdriver
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": driver.execute_script("return navigator.userAgent").replace('Headless', '')
    })
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver

# Busca o CNPJ da empresa
def buscar_cnpj(driver, nome_empresa):
    wait = WebDriverWait(driver, 10)
    try:
        driver.get("https://www.consultascnpj.com/")
        input_box = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='text']"))
        )
        input_box.clear()
        input_box.send_keys(nome_empresa)

        driver.find_element(
            By.XPATH,
            "/html/body/main/div/div[1]/form/table/tbody/tr/td[2]/button"
        ).click()

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
        print(f"[ERRO Selenium] {e}")
        return None

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
    except:
        pass
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
        print(f"[!] Erro ao ler Excel: {e}")
        return

    driver = configurar_driver()
    resultados_finais = []

    for index, row in df_input.iterrows():
        nome_original = row['company_name']

        nome_busca_normalizado = normalizar_nome(nome_original)

        print(f"\n[{index + 1}/{len(df_input)}] Buscando: {nome_busca_normalizado}")

        info_empresa = row.to_dict()
        info_empresa["Site Encontrado"] = "Não encontrado"

        # busca cnpj
        cnpj = buscar_cnpj(driver, nome_busca_normalizado)

        if cnpj:
            cnpj = str(cnpj).zfill(14)
            print(f"CNPJ: {cnpj}")

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
                    info_empresa["Site Encontrado"] = buscar_site_google(razao, cidade)
                else:
                    info_empresa["Erro_Log"] = f"BrasilAPI Status {res_api.status_code}"
            except Exception as e:
                info_empresa["Erro_Log"] = str(e)
        else:
            info_empresa["Erro_Log"] = "CNPJ não encontrado no Selenium"

        resultados_finais.append(info_empresa)
        time.sleep(1)  # delay para evitar bloqueios por IP

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
    print(f"\nProcessamento concluído! arquivo salvo: {ARQUIVO_SAIDA}")


if __name__ == "__main__":
    processar_base()