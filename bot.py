import json
import os
import urllib.request


def enviar_mensagem():
    token = os.environ["BOT_TOKEN"]
    canal = os.environ["CHAT_ID"]
    mensagem = "✅ Capunga Dados Bot: teste de ligação concluído!"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    dados = json.dumps({
        "chat_id": canal,
        "text": mensagem
    }).encode("utf-8")

    pedido = urllib.request.Request(
        url,
        data=dados,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(pedido, timeout=20) as resposta:
        resultado = json.loads(resposta.read().decode("utf-8"))
        if not resultado.get("ok"):
            raise RuntimeError("O Telegram não confirmou o envio.")
        print("Mensagem enviada com sucesso.")


if __name__ == "__main__":
    enviar_mensagem()
import os
import requests

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]

def enviar_mensagem(texto):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resposta = requests.post(
        url,
        data={"chat_id": CHAT_ID, "text": texto},
        timeout=20
    )
    resposta.raise_for_status()

if __name__ == "__main__":
    enviar_mensagem(
        "🤖 Capunga Dados Bot está online!\n"
        "⚠️ A ligação ao Telegram foi iniciada.\n"
        "Os sinais Bac bo ainda não estão ligados aos resultados em tempo real."
    )
