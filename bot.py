
import json
import os
import urllib.request


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
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(pedido, timeout=20) as resposta:
        resultado = json.loads(resposta.read().decode("utf-8"))

    if not resultado.get("ok"):
        raise RuntimeError("Falha ao enviar mensagem")


if __name__ == "__main__":
    enviar_mensagem(
        "🤖 Capunga Dados Bot\n"
        "✅ Telegram ligado com sucesso!\n"
        "⏳ Leitura automática do Bac bo ainda em configuração."
    )
    
