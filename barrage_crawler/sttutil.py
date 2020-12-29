import logging
from typing import Tuple, Iterator
from struct import Struct
from json import loads

logger = logging.getLogger("crawler.sttutil")


class STTUtil:

    STT_MSG_LENGTH_BSIZE = 4
    STT_MSG_TYPE_BSIZE = 2
    STT_MSG_ENCRYPT_BSIZE = 1
    STT_MSG_OTHER_BSIZE = 1

    STT_MSG_DOWNSTREAM_TYPE = 690
    STT_MSG_UPSTREAM_TYPE = 689

    _header_size = (
        STT_MSG_LENGTH_BSIZE
        + STT_MSG_TYPE_BSIZE
        + STT_MSG_ENCRYPT_BSIZE
        + STT_MSG_OTHER_BSIZE
    )
    _header = Struct("<2IH2B")

    @classmethod
    def _pack_header(
        cls,
        msg_length: int,
        msg_type: int,
        msg_encrypt: int = 0,
        msg_other: int = 0,
    ) -> bytes:
        return cls._header.pack(
            msg_length, msg_length, msg_type, msg_encrypt, msg_other
        )

    @classmethod
    def _unpack_header(cls, msg_header: bytes) -> Tuple[int, int, int, int]:
        (
            _,
            msg_length,
            msg_type,
            msg_encrypt,
            msg_other,
        ) = cls._header.unpack_from(msg_header)
        return msg_length, msg_type, msg_encrypt, msg_other

    @classmethod
    def pack(cls, msg_body: str) -> bytes:
        body = msg_body.encode("utf-8")
        zero_byte = b"\x00"
        msg_length = cls._header_size + len(body) + 1
        msg_header = cls._pack_header(msg_length, cls.STT_MSG_UPSTREAM_TYPE)
        return msg_header + body + zero_byte

    @classmethod
    def unpack_from(cls, frame: bytes) -> Iterator[str]:
        p_head = 0
        p_tail = 0
        frame_length = len(frame)
        while p_head != frame_length:
            msg_length, _, _, _ = cls._unpack_header(
                frame[
                    p_head : p_head
                    + cls._header_size
                    + cls.STT_MSG_LENGTH_BSIZE
                ]
            )
            p_tail = p_head + cls.STT_MSG_LENGTH_BSIZE + msg_length
            body = frame[
                p_head
                + cls.STT_MSG_LENGTH_BSIZE
                + cls._header_size : p_tail
                - 1
            ]
            yield body.decode(encoding="utf-8", errors="ignore")
            p_head = p_tail

    @classmethod
    def stt_parse(cls, msg: str):
        if "@=" in msg:
            # is dict
            items = msg.split("/")[0:-1]
            if len(items):
                res = {}
                for item in items:
                    if item and "@=" in item:
                        k, v = item.split("@=", 1)
                        res[cls._unescape(k)] = cls.stt_parse(cls._unescape(v))
                return res
            else:
                return None
            # when value is an url, it causes problems
            # is list
            # else:
            # return [cls.stt_parse(cls._unescape(i)) for i in items]
        else:
            # is string
            return cls._unescape(msg)

    @classmethod
    def stt_parses(cls, msg: str):
        msg = msg.replace("@=", '":"').replace("/", '","')
        msg = cls._unescape(msg)
        msg = f'{{"{msg[:-2]}}}'
        return loads(msg)

    @classmethod
    def stt_render(cls, msg: dict) -> str:
        attrs = ["@=".join(map(cls._escape, kv)) for kv in msg.items()]
        return "/".join(attrs) + "/"

    @staticmethod
    def _escape(s: str) -> str:
        return s.replace("@", "@A").replace("/", "@S")

    @staticmethod
    def _unescape(s: str) -> str:
        return s.replace("@A", "@").replace("@S", "/")
