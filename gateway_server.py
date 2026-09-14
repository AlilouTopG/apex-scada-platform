import asyncio
import logging
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# محاولة استيراد مكتبة pymodbus
try:
    from pymodbus.client import ModbusTcpClient
except ImportError:
    ModbusTcpClient = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IndustrialGateway")

app = FastAPI(title="APEX SCADA WebSocket & Modbus Bridge")

# السماح للواجهة على Vercel بالاتصال بهذا السيرفر
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # يمكن استبدالها برابط موقعك على Vercel حصراً للأمان
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# إدارة الاتصالات النشطة مع المتصفح
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"Client connected: {websocket.client}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnected: {websocket.client}")

    async def broadcast(self, data: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(data)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")

manager = ConnectionManager()

# إعدادات الاتصال بالـ PLC الفعلي أو المحاكي المحلي
PLC_HOST = "127.0.0.1"
PLC_PORT = 5020  # أو 502 للبروتوكول القياسي
SLAVE_ID = 1

async def modbus_polling_loop():
    """خلفية دورية لقراءة السجلات الحقيقية وبثها عبر الـ WebSocket"""
    client = None
    if ModbusTcpClient:
        client = ModbusTcpClient(PLC_HOST, port=PLC_PORT, timeout=1.0)

    while True:
        pv_val = 50.0
        mv_val = 25.0
        connected = False

        if client:
            try:
                if not client.is_socket_open():
                    client.connect()
                
                # قراءة سجلات الـ Holding (مثال: العنوان 0 بقراءة سجلين PV و MV)
                res = client.read_holding_registers(address=0, count=2, slave=SLAVE_ID)
                if not res.isError():
                    raw_pv, raw_mv = res.registers[0], res.registers[1]
                    # تحويل القيم الخام (Siemens Scale 0-27648) إلى نسب مئوية
                    pv_val = round((raw_pv / 27648.0) * 100.0, 2)
                    mv_val = round((raw_mv / 27648.0) * 100.0, 2)
                    connected = True
            except Exception as ex:
                logger.warning(f"Modbus polling warning: {ex}")
                connected = False

        payload = {
            "timestamp": asyncio.get_event_loop().time(),
            "pv": pv_val,
            "mv": mv_val,
            "connected": connected,
            "source": f"Modbus TCP ({PLC_HOST}:{PLC_PORT})"
        }

        await manager.broadcast(payload)
        await asyncio.sleep(0.5)  # معدل تحديث مرتين في الثانية

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(modbus_polling_loop())

@app.websocket("/ws/telemetry")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # استماع للأوامر القادمة من الواجهة (مثل كتابة Setpoint أو MV)
            data = await websocket.receive_json()
            logger.info(f"Received command from UI: {data}")
            # هنا يمكنك إضافة كود كتابة سجل Modbus (write_register) عند استقبال أوامر من المتصفح
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    uvicorn.run("gateway_server:app", host="127.0.0.1", port=8000, reload=True)