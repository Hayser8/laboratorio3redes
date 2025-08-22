import socket, threading, json
from utils import log

def send_json_line(host:str, port:int, obj:dict, timeout:float=2.0):
    data = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.sendall(data)
    finally:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        s.close()

class JsonLineServer(threading.Thread):
    def __init__(self, host:str, port:int, handler, name:str="server"):
        super().__init__(daemon=True, name=name)
        self.host = host
        self.port = port
        self.handler = handler
        self._stop = threading.Event()

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(100)
        log(f"[transport] listening on {self.host}:{self.port}")
        srv.settimeout(0.5)
        while not self._stop.is_set():
            try:
                conn, addr = srv.accept()
                conn.settimeout(2.0)
                threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except OSError:
                break
        try:
            srv.close()
        except Exception:
            pass

    def _handle_conn(self, conn, addr):
        buff = b""
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buff += data
                while b"\n" in buff:
                    line, buff = buff.split(b"\n", 1)
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line.decode("utf-8"))
                        self.handler(obj, addr)
                    except json.JSONDecodeError as e:
                        log(f"[transport] JSON decode error from {addr}: {e}")
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def stop(self):
        self._stop.set()
