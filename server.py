import asyncio
import websockets
import json
import logging
import sys

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout
)
log = logging.getLogger("WebSocketServer")


class Client:
    def __init__(self, websocket):
        self.websocket = websocket
        self.room = None
        self.players = []
        self.ready = False

    async def send(self, message):
        try:
            await self.websocket.send(json.dumps(message))
        except Exception as e:
            log.warning(f"Send failed: {e}")

    async def handle_join(self, data):
        room_name = data.get("room")
        if not room_name:
            await self.send({"error": "Missing 'room'"})
            return

        players = data.get("players", [])
        self.players = players

        if self.room:
            self.room.remove(self)

        room = server.get_or_create_room(room_name)
        room.add(self)

    async def handle_rooms(self, data):
        rooms = list(server.rooms.keys())
        asyncio.create_task(self.send({
            "action": "rooms",
            "rooms": rooms
        }))

        log.info("Client requested list of rooms")

class Room:
    def __init__(self, name):
        self.name = name
        self.clients = set()
        self.scores = {}
        self.state = "waiting"

    async def handle_score(self, data, client):
        if not self.state == "playing":
            await client.send({"error": "Game not started yet"})
            return

        name = data.get("name")
        score = data.get("score")
        failed = data.get("failed")

        if name is None or score is None or failed is None:
            await client.send({"error": "Missing 'name', 'score', or 'failed'"})
            return

        await self.update_score(name, score, failed)

    async def handle_players(self, data, client):
        self.broadcast_players()

    async def handle_start(self, data, client):
        for c in self.clients:
            if not c.ready:
                await client.send({"error": "Not all players are ready"})
                return
            
        if not self.state == "waiting":
            await client.send({"error": "Game already started"})
            return
        
        self.state = "playing"
        
        await asyncio.gather(
            self.broadcast({"action": "start"}),
            server.broadcast_rooms()
        )

        log.info(f"[{self.name}] Game started")

    async def handle_leave(self, data, client):
        log.info(f"[{self.name}] Client requested to leave room")
        self.remove(client)

    async def handle_ready(self, data, client):
        if not self.state == "waiting":
            await client.send({"error": "Cannot ready up, game already started"})
            return
        
        if 'ready' not in data or not isinstance(data['ready'], bool):
            await client.send({"error": "Missing 'ready' status"})
            return

        client.ready = data['ready']
        self.broadcast_players()

    def add(self, client):
        if client in self.clients:
            log.warning(f"Client already in room '{self.name}'")
            return

        self.clients.add(client)
        client.room = self
        log.info(f"Client joined room '{self.name}'")
        self.broadcast_players()

    def remove(self, client):
        self.clients.discard(client)
        client.room = None
        log.info(f"Client left room '{self.name}'")

        if self.state == "playing":
            for p in client.players:
                if p in self.scores:
                    self.scores[p]["failed"] = True

        if self.is_empty():
            log.info(f"Room '{self.name}' is empty, removing it")
            del server.rooms[self.name]
        else:
            self.broadcast_players()

    def broadcast_players(self):
        players = []
        for client in self.clients:
            for player in client.players:
                players.append({
                    "name": player,
                    "ready": client.ready
                })

        players.sort(key=lambda x: x["name"].lower())  # Sort by player name
        log.info(f"[{self.name}] Broadcasting players: {players}")

        asyncio.create_task(self.broadcast({
            "action": "players",
            "players": players
        }))

    def is_empty(self):
        return len(self.clients) == 0

    async def broadcast(self, message):
        if not self.clients:
            log.warning(f"[{self.name}] No clients to broadcast to")
            return
        # log.info(f"[{self.name}] Broadcasting message: {message}")
        await asyncio.gather(*[client.send(message) for client in self.clients])

    async def update_score(self, name, score, failed):
        self.scores[name] = {"score": score, "failed": failed}
        # log.info(f"[{self.name}] Score updated: {name} => {score}, failed={failed}")
        # print(self.scores)  # Debug print to see scores in console

        # await asyncio.sleep(0.1)  # Simulate some processing delay

        scoresSend = []
        for player, data in self.scores.items():
            scoresSend.append({
                "player": player,
                "score": data["score"],
                "failed": data["failed"]
            })

        scoresSend.sort(key=lambda x: float(x["score"]), reverse=True)

        await self.broadcast({
            "action": "scores",
            "scores": scoresSend
        })


class Server:
    def __init__(self):
        self.rooms = {}  # room name -> Room
        self.clients = {}  # websocket -> Client

    async def broadcast_rooms(self):
        free_clients = [client for client in self.clients.values() if not client.room]
        rooms = list([r.name for r in self.rooms.values() if r.state == "waiting"])

        log.info(f"Broadcasting rooms: {rooms} to {len(free_clients)} free clients")

        await asyncio.gather(*[client.send({
            "action": "rooms",
            "rooms": rooms
        }) for client in free_clients])

    def get_or_create_room(self, name):
        if name not in self.rooms:
            self.rooms[name] = Room(name)
            log.info(f"Created room: '{name}'")

            asyncio.create_task(self.broadcast_rooms())  # Notify clients about the new room
        return self.rooms[name]

    async def disconnect(self, ws):
        client = self.clients.pop(ws, None)
        if client:
            room = client.room
            if room:
                room.remove(client)
            log.info(f"Client disconnected")

    async def handle_message(self, ws, data):
        log.debug(f"Received message: {data}")

        if ws not in self.clients:
            log.info("New client connected")
            client = self.clients.get(ws)
            client = Client(ws)
            self.clients[ws] = client
        else:
            client = self.clients[ws]

        action = data.get("action")
        if not action:
            await ws.send(json.dumps({"error": "Missing 'action'"}))
            return

        handler = getattr(client, f"handle_{action}", None)
        if handler:
            await handler(data)
            return

        if client.room:
            handler = getattr(client.room, f"handle_{action}", None)
            
            if handler:
                await handler(data, client)
                return
        else:
            if action == "leave":
                return
            await ws.send(json.dumps({"error": f"Unknown action: {action} not in a room?"}))
            log.warning(f"Unknown action: {action}")
            

    async def handler(self, ws):
        log.info("Client connected")
        try:
            async for msg in ws:
                try:
                    data = json.loads(msg)
                    await self.handle_message(ws, data)
                except json.JSONDecodeError:
                    await ws.send(json.dumps({"error": "Invalid JSON"}))
                    log.warning("Invalid JSON received")
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.disconnect(ws)

    async def start(self, host="0.0.0.0", port=8765):
        async with websockets.serve(self.handler, host, port, open_timeout=5, ping_interval=5, ping_timeout=5, close_timeout=5):
            log.info(f"Server started on ws://{host}:{port}")
            await asyncio.Future()  # run forever


if __name__ == "__main__":
    server = Server()
    asyncio.run(server.start())
