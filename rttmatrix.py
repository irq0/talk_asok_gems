source="/compile2/ceph/wip"
build="/compile2/ceph/wip/build"
out="/compile2/ceph/wip/build/out"
asok="/compile2/ceph/wip/build/asok"
ceph_config="/compile2/ceph/wip/build/ceph.conf"
run_id="297838a4-5a65-4c97-a708-43f35c4b1f46"
#!/usr/bin/env python3
import rados
import cephfs
import json
import pathlib
import logging
import socket
from typing import Annotated, Any, Literal, NamedTuple, Optional, Tuple, Union, cast, override

LOG = logging.getLogger("asok")
CEPH_COMMAND_TIMEOUT_SECONDS = 0

from pydantic import (
    BaseModel,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
)

class CephTargetBase(BaseModel):
    class Config:
        frozen = True


class CephOSDTarget(CephTargetBase):
    type: Literal["osd"]
    id: int

    def to_tuple(self):
        return (self.type, self.id)

    @override
    def __str__(self) -> str:
        return f"osd.{self.id}"


class CephMonTarget(CephTargetBase):
    type: Literal["mon"]
    name: str

    def to_tuple(self):
        return (self.type, self.name)

    @override
    def __str__(self) -> str:
        if self.name:
            return f"mon.{self.name}"
        else:
            return "mon"


class CephMgrTarget(CephTargetBase):
    type: Literal["mgr"]
    name: str

    def to_tuple(self):
        return (self.type, self.name)

    @override
    def __str__(self) -> str:
        return f"mgr.{self.name}"


class CephAsokTarget(CephTargetBase):
    type: Literal["asok"]
    path: pathlib.Path

    def to_tuple(self):
        return (self.type, self.path)

    @override
    def __str__(self) -> str:
        return self.path.name

class CephMdsTarget(CephTargetBase):
    type: Literal["mds"]
    name: str

    def to_tuple(self):
        return (self.type, self.path)

    @override
    def __str__(self) -> str:
        return f"mds.{self.name}"

CephTarget = Annotated[
    CephOSDTarget | CephMonTarget | CephMgrTarget | CephAsokTarget | CephMdsTarget,
    Field(discriminator="type"),
]

ConfigVariant = Union[StrictBool, StrictInt, StrictFloat, StrictStr]

class CephCommandError(Exception):
    pass

def asok_command(path: pathlib.Path, cmd: str):
    cmd += "\0"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(path.as_posix())
        LOG.debug("ASOK: %s --> %s", path, cmd)
        sock.sendall(cmd.encode("utf-8"))
        response_bytes = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response_bytes += chunk
        LOG.debug("ASOK: %s <-- %s", path, response_bytes)
    if b"ERROR:" in response_bytes:
        raise CephCommandError(f'Ceph asok command "{cmd}" failed: {response_bytes}')
    return 0, response_bytes[4:], b""


def target_command(
    target: CephTarget, cluster: rados.Rados, cmd: str
) -> tuple[str, str]:
    match target:
        case CephOSDTarget(type="osd", id=osdid):
            ret, outs, outbuf = cluster.osd_command(
                osdid=osdid, cmd=cmd, inbuf=b"", timeout=CEPH_COMMAND_TIMEOUT_SECONDS
            )
        case CephMonTarget(type="mon", name=monid):
            ret, outs, outbuf = cluster.mon_command(
                cmd=cmd, inbuf=b"", timeout=CEPH_COMMAND_TIMEOUT_SECONDS, target=monid
            )
        case CephMgrTarget(type="mgr", name=mgr):
            ret, outs, outbuf = cluster.mgr_command(
                cmd=cmd, inbuf=b"", timeout=CEPH_COMMAND_TIMEOUT_SECONDS, target=mgr
            )
        case CephMdsTarget(type="mds", name=mds):
            fs = cephfs.LibCephFS(rados_inst=cluster)
            fs.init()
            ret, outs, outbuf = fs.mds_command(
                mds, cmd, b""
            )
            fs.shutdown()
        case CephAsokTarget(type="asok", path=path):
            ret, outs, outbuf = asok_command(path, cmd)

    LOG.debug("cmd %r ret: %r", cmd, ret)

    if ret == 0:
        if isinstance(outs, bytes):
            outs = outs.decode("utf-8")
        if isinstance(outbuf, bytes):
            outbuf = outbuf.decode("utf-8")
        return outs, outbuf
    raise CephCommandError(f'Ceph command "{cmd}" failed with {ret}: {outs}')


def command_outs(
    cluster: rados.Rados,
    target: CephTarget = CephMonTarget(type="mon", name=""),
    **kwargs: Any,
) -> str:
    outs, _ = target_command(target, cluster, json.dumps(kwargs))
    return outs.strip()


def command_json(
    cluster: rados.Rados,
    target: CephTarget = CephMonTarget(type="mon", name=""),
    **kwargs: Any,
) -> Any:
    kwargs["format"] = "json"
    outs, _ = target_command(target, cluster, json.dumps(kwargs))
    try:
        j = json.loads(outs)
    except json.JSONDecodeError as ex:
        LOG.error("JSON parse failed: %s", ex, exc_info=True)
        ex.add_note(outs)
        raise
    return j


def command_lines(
    cluster: rados.Rados,
    target: CephTarget = CephMonTarget(type="mon", name=""),
    **kwargs: Any,
) -> list[str]:
    outs, _ = target_command(target, cluster, json.dumps(kwargs))
    return [line for line in outs.splitlines() if line]

def get_inventory(cluster: rados.Rados) -> dict[str, list[CephTarget]]:
    fs_dump = command_json(cluster, CephMonTarget(type="mon", name=""), prefix="fs dump")
    return {
        "osd": [
            CephOSDTarget(type="osd", id=int(osd))
            for osd in command_lines(cluster, prefix="osd ls")
        ],
        "mon": [
            CephMonTarget(type="mon", name=m["name"])
            for m in command_json(cluster, prefix="mon dump")["mons"]
        ],
        "mgr": [
            CephMgrTarget(
                type="mgr", name=command_json(cluster, prefix="mgr dump")["active_name"]
            )
        ],
        "mds" :
            [CephMdsTarget(type="mds", name=info["name"]) for info in fs_dump["standbys"]] +
            [CephMdsTarget(type="mds", name=info["name"]) for fs in fs_dump["filesystems"] for info in fs["mdsmap"]["info"].values()],
        "rgw":
            [CephAsokTarget(type="asok", path=pathlib.Path(out, "radosgw.8000.asok"))]
    }


def connect(conffile: pathlib.Path) -> rados.Rados:
    cluster = rados.Rados(conffile=conffile.as_posix())
    cluster.connect()
    LOG.info("Connected to cluster %s", cluster.get_fsid())
    return cluster

from datetime import timedelta

class EntityNameT(BaseModel):
    type: str
    num: int


class EntityName(BaseModel):
    type: int
    id: str


class EntityAddr(BaseModel):
    type: str
    addr: str
    nonce: int

    def human(self) -> str:
        if self.type == "none":
            return "∅"
        elif self.type == "any":
            return f"#{str(self.nonce)}"
        else:
            return f"{self.type}/{self.addr}#{str(self.nonce)}"


class AddrVec(BaseModel):
    addrvec: list[EntityAddr]


class Socket(BaseModel):
    socket_fd: int | None
    worker_id: int | None


class DispatchQueue(BaseModel):
    length: int
    max_age_ago: str  # TODO support utimespan_str


class ConnectionStatus(BaseModel):
    connected: bool
    loopback: bool

    def connected_human(self) -> str:
        return "✔" if self.connected else "𐄂"


def format_timedelta_compact(d: timedelta) -> str:
    total_sec = d.total_seconds()
    if total_sec >= 1:
        return f"{total_sec:.3f} s"
    elif total_sec >= 1e-3:
        return f"{total_sec * 1e3:.3f} ms"
    elif total_sec == 0:
        return "0"
    else:
        return f"{total_sec * 1e6:.0f} µs"


class TCPInfo(BaseModel):
    tcpi_state: str
    tcpi_retransmits: int
    tcpi_probes: int
    tcpi_backoff: int
    tcpi_rto: timedelta = Field(alias="tcpi_rto_us")
    tcpi_ato: timedelta = Field(alias="tcpi_ato_us")
    tcpi_snd_mss: int
    tcpi_rcv_mss: int
    tcpi_unacked: int
    tcpi_lost: int
    tcpi_retrans: int
    tcpi_pmtu: int
    tcpi_rtt: timedelta = Field(alias="tcpi_rtt_us")
    tcpi_rttvar: timedelta = Field(alias="tcpi_rttvar_us")
    tcpi_total_retrans: int
    tcpi_last_data_sent: timedelta = Field(alias="tcpi_last_data_sent_ms")
    tcpi_last_ack_sent: timedelta = Field(alias="tcpi_last_ack_sent_ms")
    tcpi_last_data_recv: timedelta = Field(alias="tcpi_last_data_recv_ms")
    tcpi_last_ack_recv: timedelta = Field(alias="tcpi_last_ack_recv_ms")
    tcpi_options: list[str]

    @field_validator("tcpi_rto", "tcpi_ato", "tcpi_rtt", "tcpi_rttvar", mode="before")
    @classmethod
    def us_timedelta(cls, value: int) -> timedelta:
        return timedelta(milliseconds=value / 1000)

    @field_validator(
        "tcpi_last_data_sent",
        "tcpi_last_ack_sent",
        "tcpi_last_data_recv",
        "tcpi_last_ack_recv",
        mode="before",
    )
    @classmethod
    def ms_timedelta(cls, value: int) -> timedelta:
        return timedelta(milliseconds=value)

    def human(self, k: str) -> str:
        v = getattr(self, k)
        if v:
            if isinstance(v, timedelta):
                return format_timedelta_compact(v)
            else:
                return str(v)
        else:
            return ""


class Peer(BaseModel):
    entity_name: EntityName
    type: str
    id: int
    global_id: int
    addr: AddrVec

    def human(self):
        return f"{self.global_id}/{self.id}" if self.id != -1 else "∅"


class ProtocolV2Crypto(BaseModel):
    rx: str
    tx: str


class ProtocolV2Compression(BaseModel):
    rx: str
    tx: str


class ProtocolV1(BaseModel):
    state: str
    connect_seq: int
    peer_global_seq: int
    con_mode: Optional[str]


class ProtocolV2(BaseModel):
    state: str
    connect_seq: int
    peer_global_seq: int
    con_mode: Optional[str]
    rev1: bool
    crypto: ProtocolV2Crypto
    compression: ProtocolV2Compression


class Protocol(BaseModel):
    v1: Optional[ProtocolV1] = None
    v2: Optional[ProtocolV2] = None

    def crypto(self) -> str:
        if self.v2:
            crypto = self.v2.crypto
            if crypto.rx == crypto.tx:
                return crypto.rx
            else:
                return f"{crypto.rx}/{crypto.tx}"
        else:
            return "-"

    def compression(self) -> str:
        if self.v2:
            comp = self.v2.compression
            if comp.rx == comp.tx:
                return comp.rx
            else:
                return f"{comp.rx}/{comp.tx}"
        else:
            return "-"

    def mode(self) -> str:
        if self.v2:
            return self.v2.con_mode or ""
        if self.v1:
            return self.v1.con_mode or ""
        return ""


class AsyncConnection(BaseModel):
    state: str
    messenger_nonce: int
    status: ConnectionStatus
    socket_fd: int | None
    tcp_info: TCPInfo | None
    conn_id: int
    peer: Peer
    last_connect_started_ago: str  # TODO support timepan_str
    last_active_ago: str
    recv_start_time_ago: str
    last_tick_id: int
    socket_addr: EntityAddr
    target_addr: EntityAddr
    port: int
    protocol: Protocol
    worker_id: int


class Connection(BaseModel):
    addrvec: list[EntityAddr]
    async_connection: AsyncConnection


class Messenger(BaseModel):
    nonce: int
    my_name: EntityNameT
    my_addrs: AddrVec
    listen_sockets: list[Socket] = []
    dispatch_queue: DispatchQueue
    connections_count: int
    connections: list[Connection]
    anon_conns: list[AsyncConnection]
    accepting_conns: list[AsyncConnection]
    deleted_conns: list[AsyncConnection]
    local_connection: list[AsyncConnection]

    def direction(self, connection: AsyncConnection):
        if connection.socket_addr in self.my_addrs.addrvec:
            return "IN"
        else:
            return "OUT"


class MessengerDump(BaseModel):
    name: str
    messenger: Messenger

def discover_messengers(cluster: rados.Rados, target: CephTarget) -> list[str]:
    try:
        return command_json(cluster, target, prefix="messenger dump")["messengers"]
    except CephCommandError:
        LOG.error(
            'Failed to discover messengers on %s. "messenger dump" supported?',
            target,
        )
        return []


def dump_messenger(
    cluster: rados.Rados, target: CephTarget, msgr: str
) -> Messenger | None:
    try:
        return MessengerDump.model_validate_json(
            target_command(
                target,
                cluster,
                json.dumps(
                    {
                        "prefix": "messenger dump",
                        "msgr": msgr,
                        "tcp_info": True,
                        "dumpcontents:all": True,
                    }
                ),
            )[0]
        ).messenger
    except CephCommandError as ex:
        LOG.error('Messenger "%s" dump on %s failed: %s', msgr, target, ex)
        return None

def dump_messengers(
    cluster: rados.Rados, target: CephTarget, msgrs: list[str]
) -> dict[str, Messenger]:
    result: dict[str, Messenger] = {}
    for msgr in msgrs:
        dump = dump_messenger(cluster, target, msgr)
        if dump:
            result[msgr] = dump
    return result


from operator import itemgetter
from functools import reduce
from collections import defaultdict
from itertools import permutations, chain
import matplotlib.pyplot as plt
import numpy as np
import textwrap

plt.style.use('tableau-colorblind10')

cluster = connect(pathlib.Path(ceph_config))
all_targets = list(chain(*get_inventory(cluster).values()))
n_targets = len(all_targets)
#all_targets = get_inventory(cluster)["osd"]

def format_timedelta_compact(d: timedelta) -> str:
    total_sec = d.total_seconds()
    if total_sec >= 1:
        return f"{total_sec:.0f}"
    elif total_sec >= 1e-3:
        return f"{total_sec * 1e3:.0f}m"
    elif total_sec == 0:
        return "0"
    else:
        return f"{total_sec * 1e6:.0f}µ"

def all_msgrs_outgoing_connections(target):
    msgrs = dump_messengers(cluster, target, discover_messengers(cluster, target))

    return {(c.async_connection.target_addr.type, c.async_connection.target_addr.addr, c.async_connection.target_addr.nonce):
            (m_name, c.async_connection.tcp_info.tcpi_rtt)
            for m_name, m in msgrs.items()
            for c in m.connections
            if c.async_connection.status.connected and m.direction(c.async_connection) == "OUT" and not m_name.startswith("hb_")
            }

def all_msgrs_listen(target):
    msgrs = dump_messengers(cluster, target, discover_messengers(cluster, target))
    return set([
        (a.type, a.addr, a.nonce)
        for m in msgrs.values()
        for a in m.my_addrs.addrvec])

result = defaultdict(list)
for src_t in all_targets:
    for dst_t in all_targets:
        src_outgoing = all_msgrs_outgoing_connections(src_t)
        dst_listen = all_msgrs_listen(dst_t)
        connections = src_outgoing.keys() & dst_listen
        for con in connections:
            src_outgoing_data = src_outgoing[con]
            result[(src_t, dst_t)].append(src_outgoing_data)

fig, ax = plt.subplots()

# X axis: destinations
# Y axis: sources

ax.set_title("Max RTT Source → Destination [seconds]")
ax.set_xticks(range(n_targets), labels=(str(t) for t in all_targets), rotation=45, ha="right", rotation_mode="anchor", fontsize=8)
ax.set_xlabel("Destination")
ax.set_yticks(range(n_targets), labels=reversed([str(t) for t in all_targets]), fontsize=8)
ax.set_ylabel("Source")
ax.spines[:].set_visible(False)

val_array=[[0 for i in range(n_targets)] for j in range(n_targets)]
for x, dst in enumerate(all_targets):
    for y, src in enumerate(reversed(all_targets)):
        data = max([v[1] for v in result[(src, dst)]] + [timedelta(0)])
        if src != dst:
            ax.text(x, y, format_timedelta_compact(data), ha="center", va="center", fontsize=8, color="w")
        val_array[y][x] = data.total_seconds()

im = ax.imshow(val_array)
fig.tight_layout()
return fig
