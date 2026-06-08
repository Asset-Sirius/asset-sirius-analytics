"""Funções para download, extração e processamento do Relatório Focus do BC."""

from __future__ import annotations

import re
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from typing import Optional

import pandas as pd

try:
    import pdfplumber
except ImportError:
    pdfplumber = None


USER_AGENT = "Mozilla/5.0 (compatible; FocusDownloader/1.0)"
BASE_URL = "https://www.bcb.gov.br/publicacoes/focus/cronologicos"
ARQUIVO_GOLD_INCREMENTAL = "focus_relatorios.csv"
INDICADORES_PRINCIPAIS = {
    "IPCA": "ipca",
    "PIB Total": "pib",
    "Câmbio": "cambio",
    "Selic": "selic",
}


def criar_contexto_ssl() -> ssl.SSLContext:
    """Cria contexto SSL para HTTPS com cadeia de certificados confiáveis."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


SSL_CONTEXT = criar_contexto_ssl()


@dataclass(frozen=True)
class RelatorioFocus:
    url: str
    nome: str
    data_modificacao: datetime | None


@dataclass(frozen=True)
class TabelaFocusBruta:
    pagina: int
    linhas: list[list[str | None]]


def log(nivel: str, mensagem: str) -> None:
    """Imprime mensagem de execução com nível e timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{nivel}] {mensagem}")


def obter_relatorio_mais_recente(timeout: int = 30, url_direta: Optional[str] = None) -> Optional[RelatorioFocus]:
    """Obtém o relatório Focus mais recente."""
    try:
        if url_direta:
            log("INFO", f"Usando URL fornecida: {url_direta}")
            nome = Path(urlparse(url_direta).path).name
            data_str = re.search(r'R(\d{8})', nome)
            data_modificacao = None
            if data_str:
                try:
                    data_modificacao = datetime.strptime(data_str.group(1), "%Y%m%d")
                except ValueError:
                    pass
            return RelatorioFocus(url=url_direta, nome=nome, data_modificacao=data_modificacao)
        log("INFO", "Tentando localizar relatório via heurística de nomes de arquivo (últimos 30 dias)")
        heuristica = encontrar_relatorio_por_padrao(dias_busca=30)
        if heuristica:
            return heuristica

        log("AVISO", "Nenhum relatório Focus encontrado pela heurística")
        log("INFO", "Forneça a URL direta ao pipeline se desejar usar outro arquivo.")
        return None

    except Exception as e:
        log("ERRO", f"Erro ao buscar relatório: {str(e)}")
        return None





def url_existe(url: str, timeout: int = 15) -> bool:
    """Verifica se uma URL existe usando HEAD (com fallback para GET parcial)."""
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT}, method="HEAD")
        with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
            code = getattr(resp, "status", None) or getattr(resp, "getcode", lambda: None)()
            return code == 200
    except Exception as e:
        # Se o servidor não aceitar HEAD, tenta GET de apenas o primeiro byte
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT, "Range": "bytes=0-0"})
            with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
                code = getattr(resp, "status", None) or getattr(resp, "getcode", lambda: None)()
                return code in (200, 206)
        except Exception:
            return False


def is_pdf_url(url: str, timeout: int = 15) -> bool:
    """Verifica se a URL é um PDF lendo apenas os primeiros bytes (Range 0-4)."""
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT, "Range": "bytes=0-4"})
        with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
            chunk = resp.read(5)
            if not chunk:
                return False
            return chunk.startswith(b"%PDF")
    except Exception:
        return False


def gerar_urls_candidatas(date_obj: datetime) -> list[str]:
    """Gera URLs candidatas comuns para o relatório com base na data."""
    date_str = date_obj.strftime("%Y%m%d")
    caminhos = [
        f"/content/focus/focus/R{date_str}.pdf",
        f"/publicacoes/focus/cronologicos/pdf/R{date_str}.pdf",
        f"/publicacoes/focus/cronologicos/R{date_str}.pdf",
        f"/publicacoes/focus/R{date_str}.pdf",
    ]
    return [urljoin(BASE_URL, c) for c in caminhos]


def encontrar_relatorio_por_padrao(dias_busca: int = 30) -> Optional[RelatorioFocus]:
    """Tenta localizar o relatório mais recente testando nomes de arquivo padronizados.

    Percorre as datas recentes (hoje para trás) até `dias_busca` dias e testa
    urls candidatas geradas por `gerar_urls_candidatas`.
    """
    hoje = datetime.today()
    for delta in range(dias_busca):
        data_teste = hoje - timedelta(days=delta)
        for url in gerar_urls_candidatas(data_teste):
            if url_existe(url, timeout=10) and is_pdf_url(url, timeout=10):
                nome = Path(urlparse(url).path).name
                log("INFO", f"Encontrado relatório via heurística: {url}")
                data_mod = None
                try:
                    data_mod = datetime.strptime(re.search(r'R(\d{8})', nome).group(1), "%Y%m%d")
                except Exception:
                    data_mod = data_teste
                return RelatorioFocus(url=url, nome=nome, data_modificacao=data_mod)
    return None


def baixar_pdf(url: str, destino: Path, timeout: int = 60) -> bool:
    """Realiza o download de um PDF."""
    try:
        destino.parent.mkdir(parents=True, exist_ok=True)
        
        log("INFO", f"Iniciando download: {destino.name}")
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as response:
            conteudo = response.read()
            destino.write_bytes(conteudo)
        
        # Verifica se o arquivo baixado parece ser um PDF
        tamanho_bytes = destino.stat().st_size
        try:
            with destino.open("rb") as f:
                header = f.read(5)
        except Exception:
            header = b""

        if tamanho_bytes == 0 or not header.startswith(b"%PDF"):
            log("ERRO", f"Arquivo baixado não parece ser um PDF válido: {destino.name}")
            try:
                destino.unlink()
            except Exception:
                pass
            return False

        tamanho_mb = tamanho_bytes / (1024 * 1024)
        log("INFO", f"Download concluído: {destino.name} ({tamanho_mb:.2f} MB)")
        return True
        
    except Exception as e:
        log("ERRO", f"Erro ao baixar PDF: {str(e)}")
        return False


def extrair_tabelas_pdf(caminho_pdf: Path) -> list[TabelaFocusBruta]:
    """Extrai todas as tabelas brutas de um PDF Focus."""
    if pdfplumber is None:
        log("ERRO", "pdfplumber não está instalado")
        return []
    
    try:
        tabelas: list[TabelaFocusBruta] = []
        log("INFO", f"Extraindo tabelas de {caminho_pdf.name}")
        
        with pdfplumber.open(caminho_pdf) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                page_tabelas = page.extract_tables()
                if page_tabelas:
                    for tabela in page_tabelas:
                        if len(tabela) < 2 or len(tabela[0]) < 2:
                            continue
                        tabelas.append(TabelaFocusBruta(pagina=page_num, linhas=tabela))
                    
                    tabelas_validas = [t for t in page_tabelas if len(t) >= 2 and len(t[0]) >= 2]
                    log("INFO", f"Página {page_num}: {len(tabelas_validas)} tabela(s) extraída(s)")
        
        log("INFO", f"Total de tabelas extraídas: {len(tabelas)}")
        return tabelas
        
    except Exception as e:
        log("ERRO", f"Erro ao extrair tabelas: {str(e)}")
        return []


def _normalizar_texto(valor: str | None) -> str:
    if valor is None:
        return ""
    return str(valor).replace("\n", " ").strip()


def _normalizar_indicador(rotulo: str | None) -> tuple[str, str | None]:
    texto = _normalizar_texto(rotulo)
    if not texto:
        return "", None
    match = re.match(r"^(.*?)(?:\s*\((.*)\))?$", texto)
    if not match:
        return texto, None
    indicador = match.group(1).strip()
    unidade = match.group(2).strip() if match.group(2) else None
    return indicador, unidade


def _converter_numero(valor: str | None):
    texto = _normalizar_texto(valor)
    if texto in {"", "-", "--"}:
        return None
    texto = texto.replace(".", "").replace(",", ".")
    try:
        if "." in texto:
            return float(texto)
        return int(texto)
    except ValueError:
        return texto


def _extrair_grupos(cabecalho_superior: list[str | None]) -> list[tuple[str, int, int]]:
    grupos: list[tuple[str, int, int]] = []
    for indice, valor in enumerate(cabecalho_superior):
        if indice == 0 or valor is None:
            continue
        proximo = len(cabecalho_superior)
        for seguinte in range(indice + 1, len(cabecalho_superior)):
            if cabecalho_superior[seguinte] is not None:
                proximo = seguinte
                break
        grupos.append((_normalizar_texto(valor), indice, proximo))
    return grupos


def _construir_linhas_normalizadas(tabela: TabelaFocusBruta) -> list[dict[str, object]]:
    linhas = tabela.linhas
    if len(linhas) < 3:
        return []

    cabecalho_superior = linhas[0]
    cabecalho_inferior = linhas[1]
    grupos = _extrair_grupos(cabecalho_superior)
    linhas_saida: list[dict[str, object]] = []

    for linha in linhas[2:]:
        indicador_bruto = _normalizar_texto(linha[0] if linha else None)
        if not indicador_bruto:
            continue
        if indicador_bruto.startswith("*") or indicador_bruto.startswith("▲") or indicador_bruto.startswith("▼"):
            continue

        indicador, unidade = _normalizar_indicador(indicador_bruto)

        for grupo, inicio, fim in grupos:
            subset = linha[inicio:fim]
            valores: dict[str, object] = {
                "ha_4_semanas": _converter_numero(subset[0]) if len(subset) > 0 else None,
                "ha_1_semana": _converter_numero(subset[1]) if len(subset) > 1 else None,
                "hoje": _converter_numero(subset[2]) if len(subset) > 2 else None,
                "comp_semanal": (_normalizar_texto(subset[3]) or None) if len(subset) > 3 else None,
                "comp_semanal_semanas": _converter_numero(subset[4]) if len(subset) > 4 else None,
                "resp_30d": _converter_numero(subset[5]) if len(subset) > 5 else None,
                "valor_5_dias_uteis": _converter_numero(subset[6]) if len(subset) > 6 else None,
                "resp_5d_uteis": _converter_numero(subset[7]) if len(subset) > 7 else None,
            }

            if any(v is not None for v in valores.values()):
                linhas_saida.append(
                    {
                        "data_relatorio": None,
                        "pagina": tabela.pagina,
                        "escopo": "anual" if grupo in {"2026", "2027", "2028", "2029"} else "mensal",
                        "periodo": grupo,
                        "indicador": indicador,
                        "unidade": unidade,
                        **valores,
                    }
                )

    return linhas_saida


def processar_focus_gold(tabelas: list[TabelaFocusBruta], data_relatorio: datetime) -> pd.DataFrame:
    """Transforma tabelas do Focus em uma linha por relatório."""
    if not tabelas:
        log("ERRO", "Nenhuma tabela para processar")
        return pd.DataFrame()
    
    try:
        log("INFO", "Processando tabelas para formato gold por relatório")

        linhas_normalizadas: list[dict[str, object]] = []
        for tabela in tabelas:
            linhas_normalizadas.extend(_construir_linhas_normalizadas(tabela))

        df_normalizado = pd.DataFrame(linhas_normalizadas)
        if df_normalizado.empty:
            return df_normalizado

        df_base = df_normalizado[
            (df_normalizado["escopo"] == "anual") & (df_normalizado["periodo"] == "2026")
        ].copy()

        if df_base.empty:
            log("AVISO", "Não foi possível localizar a tabela anual de 2026 no PDF")
            return pd.DataFrame()

        df_base["indicador_base"] = df_base["indicador"].map(INDICADORES_PRINCIPAIS)
        df_base = df_base[df_base["indicador_base"].notna()]

        linha_relatorio: dict[str, object] = {
            "data_relatorio": data_relatorio.date().isoformat(),
            "data_extracao": datetime.now().isoformat(timespec="seconds"),
            "pagina": int(df_base["pagina"].min()),
            "periodo_ref": "2026",
        }

        for _, linha in df_base.iterrows():
            prefixo = linha["indicador_base"]
            linha_relatorio[f"{prefixo}_ha_1_semana"] = linha.get("ha_1_semana")
            linha_relatorio[f"{prefixo}_hoje"] = linha.get("hoje")

        for prefixo in INDICADORES_PRINCIPAIS.values():
            linha_relatorio.setdefault(f"{prefixo}_ha_1_semana", None)
            linha_relatorio.setdefault(f"{prefixo}_hoje", None)

        df_consolidado = pd.DataFrame([linha_relatorio])
        df_consolidado = df_consolidado[
            [
                "data_relatorio",
                "data_extracao",
                "pagina",
                "periodo_ref",
                "ipca_ha_1_semana",
                "ipca_hoje",
                "pib_ha_1_semana",
                "pib_hoje",
                "cambio_ha_1_semana",
                "cambio_hoje",
                "selic_ha_1_semana",
                "selic_hoje",
            ]
        ]

        log("INFO", f"Tabela gold processada: {df_consolidado.shape[0]} linha x {df_consolidado.shape[1]} colunas")
        return df_consolidado
        
    except Exception as e:
        log("ERRO", f"Erro ao processar tabelas para gold: {str(e)}")
        return pd.DataFrame()


def salvar_gold(df: pd.DataFrame, diretorio_saida: Path) -> Optional[Path]:
    """Salva a tabela gold incremental em CSV único, substituindo o mesmo relatório."""
    try:
        diretorio_saida.mkdir(parents=True, exist_ok=True)
        
        nome_arquivo = ARQUIVO_GOLD_INCREMENTAL
        caminho_saida = diretorio_saida / nome_arquivo

        if caminho_saida.exists():
            df_existente = pd.read_csv(caminho_saida, sep=';')
            df_combined = pd.concat([df_existente, df], ignore_index=True, sort=False)
            if "data_relatorio" in df_combined.columns:
                df_combined = df_combined.drop_duplicates(subset=["data_relatorio"], keep="last")
        else:
            df_combined = df.copy()

        # Ordena por data do relatório para manter histórico consistente.
        if "data_relatorio" in df_combined.columns:
            df_combined = df_combined.sort_values(by=["data_relatorio"], kind="stable")

        # Usa ponto-e-vírgula como separador para evitar quebra causada por vírgulas
        # dentro de campos (ex: decimais com vírgula). Mantemos aspas padrão.
        df_combined.to_csv(caminho_saida, index=False, encoding='utf-8', sep=';')
        log("INFO", f"Tabela gold salva: {caminho_saida}")
        return caminho_saida
        
    except Exception as e:
        log("ERRO", f"Erro ao salvar tabela gold: {str(e)}")
        return None


def deletar_arquivo(caminho: Path) -> bool:
    """Deleta um arquivo do disco."""
    try:
        if caminho.exists():
            caminho.unlink()
            log("INFO", f"Arquivo deletado: {caminho.name}")
            return True
        return False
    except Exception as e:
        log("ERRO", f"Erro ao deletar arquivo {caminho.name}: {str(e)}")
        return False


def processar_relatorio_focus(
    diretorio_temporario: Path = Path("tmp_focus"),
    diretorio_saida: Path = Path("data/gold"),
    limpar_pdf: bool = True,
    url_direta: Optional[str] = None
) -> bool:
    """Pipeline completo de processamento do Relatório Focus."""
    try:
        relatorio = obter_relatorio_mais_recente(url_direta=url_direta)
        if not relatorio:
            return False
        
        pdf_path = diretorio_temporario / relatorio.nome
        if not baixar_pdf(relatorio.url, pdf_path):
            return False
        
        tabelas = extrair_tabelas_pdf(pdf_path)
        if not tabelas:
            log("AVISO", "Nenhuma tabela foi extraída do PDF")
            deletar_arquivo(pdf_path)
            return False
        
        df_gold = processar_focus_gold(tabelas, relatorio.data_modificacao or datetime.now())
        if df_gold.empty:
            log("AVISO", "Tabela gold está vazia")
            deletar_arquivo(pdf_path)
            return False
        
        caminho_salvo = salvar_gold(df_gold, diretorio_saida)
        if not caminho_salvo:
            deletar_arquivo(pdf_path)
            return False
        
        if limpar_pdf:
            deletar_arquivo(pdf_path)
        
        log("INFO", "Pipeline concluído com sucesso!")
        return True
        
    except Exception as e:
        log("ERRO", f"Erro geral no pipeline: {str(e)}")
        return False
