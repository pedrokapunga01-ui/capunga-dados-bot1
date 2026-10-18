
import json
import os
import re
import urllib.request
from html.parser import HTMLParser


class TextoHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.textos = []

    def handle_data(self, data):
        self.textos.append(data)

    def texto(self):
        return " ".join(self.textos)


def enviar_mensagem(texto):
    token = os.environ["BOT_TOKEN"]
    canal = os.environ["CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    dados = json.dumps({
        "chat_id": canal,
        "text": texto
    }).encode("utf-8")

    pedido = urllib.request.Request(
        url,
        data=dados,
        headers={"Content-Type": "application/json"}
    )

    with urllib.request.urlopen(pedido, timeout=20) as resposta:
        resposta.read()


def testar_betmind():
    url = "https://betmind.org/bacbo"
    pedido = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    try:
        with urllib.request.urlopen(pedido, timeout=30) as resposta:
            html = resposta.read().decode("utf-8", errors="replace")

        parser = TextoHTML()
        parser.feed(html)
        texto = parser.texto()

        padrao = r"Latest:\s*(\d+)\s*(Player|Banker|Tie)\s*(\d{2}:\d{2}:\d{2})"
        resultado = re.search(padrao, texto, re.IGNORECASE)

        if resultado:
            numero, cor, hora = resultado.groups()
            enviar_mensagem(
                f"🎲 BETMIND — resultado encontrado\n"
                f"Número: {numero}\n"
                f"Resultado: {cor}\n"
                f"Hora: {hora}\n"
                f"⚠️ Isto é um resultado observado, não uma previsão."
            )
        else:
            enviar_mensagem(
                "⚠️ A página abriu, mas não consegui identificar "
                "o último resultado. É necessário verificar o formato dos dados."
            )

    except Exception as erro:
        enviar_mensagem(
            f"⚠️ Não foi possível ler o BETMIND: "
            f"{type(erro).__name__}"
        )


if __name__ == "__main__":
    testar_betmind()
        
