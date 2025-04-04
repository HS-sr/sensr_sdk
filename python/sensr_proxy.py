import asyncio
import websockets
from sensr_proto.output_pb2 import OutputMessage, ZoneEvent
from google.protobuf.timestamp_pb2 import Timestamp
from sensr_proto import type_pb2  # For LabelType

SENSR_WS = "ws://localhost:5050"
PROXY_PORT = 6000
FALL_TYPE_ID = 5  # 클라이언트와 FALL로 합의된 enum 값

clients = set()

async def proxy_handler(websocket, path):
    clients.add(websocket)
    print(f"[Client Connected] {websocket.remote_address}")
    try:
        while True:
            await asyncio.sleep(1)
    except:
        pass
    finally:
        clients.remove(websocket)
        print(f"[Client Disconnected] {websocket.remote_address}")

def create_fall_event_message(original_msg: OutputMessage):
    fall_msg = OutputMessage()
    fall_msg.CopyFrom(original_msg)

    fall_event_added = False

    if original_msg.HasField('stream') and original_msg.stream.has_objects:
        for obj in original_msg.stream.objects:
            if obj.label == type_pb2.LabelType.LABEL_PEDESTRIAN:
                occupied_zone = len(obj.zone_ids)
                if 0 < occupied_zone <= 1 and obj.zone_ids[0] == 1001:
                    for zone_id in obj.zone_ids:
                        zone_event = fall_msg.event.zone.add()
                        zone_event.timestamp.CopyFrom(original_msg.timestamp)
                        zone_event.id = zone_id
                        zone_event.type = FALL_TYPE_ID  # FALL
                        zone_event.object.id = obj.id
                        zone_event.object.position.CopyFrom(obj.bbox.position)
                        zone_event.object.velocity.CopyFrom(obj.velocity)
                        zone_event.object.heading = obj.bbox.yaw  # 또는 obj.yaw_rate
                    fall_event_added = True
                    break

    return fall_msg if fall_event_added else None

async def forward_output_messages():
    async with websockets.connect(SENSR_WS) as sensr_ws:
        while True:
            raw_msg = await sensr_ws.recv()
            msg = OutputMessage()
            msg.ParseFromString(raw_msg)

            # 1. 원본 메시지 그대로 전송
            for client in clients.copy():
                try:
                    await client.send(raw_msg)
                except:
                    clients.remove(client)

            # 2. FALL 조건 충족 시 추가 전송
            fall_msg = create_fall_event_message(msg)
            if fall_msg:
                data = fall_msg.SerializeToString()
                for client in clients.copy():
                    try:
                        await client.send(data)
                    except:
                        clients.remove(client)

async def main():
    server = websockets.serve(proxy_handler, "0.0.0.0", PROXY_PORT)
    await asyncio.gather(server, forward_output_messages())

if __name__ == "__main__":
    asyncio.run(main())
