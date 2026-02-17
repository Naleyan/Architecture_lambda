import socket
import ssl

# =========================
# CONFIGURATION
# =========================
SERVER = "irc.chat.twitch.tv"
PORT = 6697  # TLS
NICK = "marthah_h"
TOKEN = "oauth:qc9wckdsq54mozcd1tfagoxl3bkg03"
CHANNEL = "#Sardoche"  # ex: #pokimane

# =========================
# CONNEXION IRC (TLS)
# =========================
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
context = ssl.create_default_context()
sock = context.wrap_socket(sock, server_hostname="irc.chat.twitch.tv")
sock.connect((SERVER, PORT))

# Authentification
sock.send(f"PASS {TOKEN}\r\n".encode("utf-8"))
sock.send(f"NICK {NICK}\r\n".encode("utf-8"))
sock.send(f"JOIN {CHANNEL}\r\n".encode("utf-8"))

print("Connecté au chat Twitch")

# =========================
# LECTURE DU FLUX
# =========================
while True:
    resp = sock.recv(2048).decode("utf-8")

    # Réponse au PING (obligatoire)
    if resp.startswith("PING"):
        sock.send("PONG :tmi.twitch.tv\r\n".encode("utf-8"))
        continue

    for line in resp.split("\r\n"):
        if "PRIVMSG" in line:
            # Format :
            # :user!user@user.tmi.twitch.tv PRIVMSG #channel :message
            prefix, message = line.split("PRIVMSG", 1)
            user = prefix.split("!")[0][1:]
            content = message.split(":", 1)[1]

            print(f"{user}: {content}")
