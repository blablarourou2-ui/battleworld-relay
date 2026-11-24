# relay.py -- serveur multijoueur de battleWorld (salons de 5 joueurs au plus)
# Il ne fait que relayer les messages entre les joueurs d'un même salon :
#   hello {name, room}  -> welcome {id, room, host, players, world} (room vide = créer un salon)
#   world {seed, ...}   -> gardé pour les suivants et envoyé aux autres (seul l'hôte l'envoie)
#   tout autre message  -> renvoyé aux autres joueurs du salon avec l'id de l'expéditeur
#   déconnexion         -> left {id} ; si l'hôte part, le suivant devient hôte (host {id})
# Lancement : python relay.py   (port : variable d'environnement PORT, 8765 par défaut)
import asyncio, json, os, random, string, http

import websockets
from websockets.asyncio.server import serve

MAX_PLAYERS = 5
rooms = {}          # code -> {"players": {id: ws}, "names": {id: name}, "host": id, "world": dict|None}
next_id = 1


def new_code():
    while True:
        c = "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(4))
        if c not in rooms:
            return c


async def send(ws, msg):
    try:
        await ws.send(json.dumps(msg))
    except Exception:
        pass


async def broadcast(room, msg, skip=None):
    for pid, ws in list(room["players"].items()):
        if pid != skip:
            await send(ws, msg)


async def handler(ws):
    global next_id
    pid = next_id; next_id += 1
    room = None; code = ""
    try:
        async for raw in ws:
            if len(raw) > 20000:
                continue
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("t")
            if room is None:
                if t != "hello":
                    continue
                name = str(msg.get("name", "Joueur"))[:16]
                code = str(msg.get("room", "")).upper().strip()[:6]
                if code == "":
                    code = new_code()
                    rooms[code] = {"players": {}, "names": {}, "host": pid, "world": None}
                elif code not in rooms:
                    await send(ws, {"t": "error", "msg": "Salon introuvable"}); continue
                elif len(rooms[code]["players"]) >= MAX_PLAYERS:
                    await send(ws, {"t": "error", "msg": "Salon plein (5 joueurs)"}); continue
                room = rooms[code]
                room["players"][pid] = ws; room["names"][pid] = name
                await send(ws, {"t": "welcome", "id": pid, "room": code, "host": room["host"] == pid,
                                "players": [{"id": i, "name": n} for i, n in room["names"].items() if i != pid],
                                "world": room["world"]})
                await broadcast(room, {"t": "joined", "id": pid, "name": name}, skip=pid)
                continue
            if t == "world":
                if room["host"] == pid:
                    room["world"] = msg
                    await broadcast(room, msg, skip=pid)
                continue
            if t == "ping":
                await send(ws, {"t": "pong"}); continue
            msg["id"] = pid
            await broadcast(room, msg, skip=pid)
    except websockets.ConnectionClosed:
        pass
    finally:
        if room is not None:
            room["players"].pop(pid, None); room["names"].pop(pid, None)
            await broadcast(room, {"t": "left", "id": pid})
            if not room["players"]:
                rooms.pop(code, None)
            elif room["host"] == pid:
                room["host"] = next(iter(room["players"]))
                await broadcast(room, {"t": "host", "id": room["host"]})


def health(connection, request):
    # Render vérifie que le service répond en HTTP : on répond « ok » à tout ce qui n'est pas un WebSocket
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(http.HTTPStatus.OK, "battleWorld relay ok\n")
    return None


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with serve(handler, "0.0.0.0", port, process_request=health, ping_interval=20, ping_timeout=40, max_size=65536) as server:
        print("relais battleWorld sur le port", port, flush=True)
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
