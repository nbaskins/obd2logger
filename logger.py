import os
import can
import cantools
from influxdb import InfluxDBClient

# load socketCAN bus and dbc file
cwd = os.getcwd()
dbc_path = os.path.join(cwd, "obd2-11bit.dbc")
dbc = cantools.database.load_file(dbc_path)
bus = can.interface.Bus(channel="can0", interface="socketcan")
bus.set_filters([{"can_id": msg.frame_id, "can_mask": 0x7FF, "extended": False} for msg in dbc.messages])

# pids dict
pids = {
    "S01PID0D_VehicleSpeed": 0x0D,
    "S01PID0C_EngineRPM": 0x0C,
    "S01PID11_ThrottlePosition": 0x11
}

# influxdb client object
client = InfluxDBClient(
    host="localhost",
    port=8086,
    username="logger",
    password="password",
    database="logger_db"
)

# writes a signal name and value to influxdb
def write_to_influx(name, value):
    json_body = [
        {
            "measurement": "obd2",
            "fields": {
                name: value
            }
        }
    ]
    client.write_points(json_body)
    print(f"Wrote {name} = {value} to InfluxDB")

# sends an obd2 request for a pid
def send_request(pid):
    data = [0x02, 0x01, pid, 0x00, 0x00, 0x00, 0x00, 0x00]
    msg = can.Message(arbitration_id=0x7DF, data=data, is_extended_id=False)
    bus.send(msg)
    print(f"Sent PID request: {hex(pid).upper()}")

# receives and handles obd2 response
def handle_response(name):
    msg = bus.recv(timeout=1.0)
    if not msg:
        print("No message received.")
        return

    pid = msg.data[2]

    if pid != pids[name]:
        print(f"Unexpected PID: {hex(pid).upper()}")
        return
    
    signal = dbc.get_message_by_frame_id(msg.arbitration_id).get_signal_by_name(name)
    raw_bytes = msg.data[3:(3 + (signal.length // 8))]
    raw_value = int.from_bytes(raw_bytes, byteorder="big")
    value = raw_value * signal.scale + signal.offset
    write_to_influx(name, value)

# main loop
try:
    while True:
        for name, pid in pids.items():
            send_request(pid)
            handle_response(name)
finally:
    bus.shutdown()